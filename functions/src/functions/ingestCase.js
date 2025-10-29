/**
 * src/functions/ingestCase.js
 * * Función 'onCall' (Etapa 1) - CORREGIDA
 * * Usa dos transacciones para cumplir la regla "lecturas antes de escrituras".
 */

import { onCall, HttpsError } from 'firebase-functions/v2/https'
import logger from 'firebase-functions/logger'
import { FieldValue, GeoPoint } from 'firebase-admin/firestore'
import { DateTime } from 'luxon'

import { db, taskQueue } from '../firebase.js'
import { config } from '../config.js'
import { paths } from '../utils/paths.js'
import {
  floorToHourISO,
  addHoursISO,
  hoursBetween,
  getHourKey,
} from '../utils/time.js'
import { toH3, toGeohash, centerWithKRing1 } from '../utils/geo.js'

// --- Lógica de Rollup (Movida a su propia transacción) ---

async function initializeRollup(
  tx,
  rollupRef,
  condition,
  h3,
  eventHourISO,
  metadata,
) {
  const pastHourKeys = []
  for (let i = 1; i < config.ROLLUP_WINDOW_HOURS; i++) {
    pastHourKeys.push(addHoursISO(eventHourISO, -i))
  }
  const pastBucketRefs = pastHourKeys.map((h) =>
    db.doc(paths.bucket1hDoc(condition, h3, h)),
  )

  // LECTURAS (al inicio de esta transacción)
  const pastBucketSnaps = pastBucketRefs.length
    ? await tx.getAll(...pastBucketRefs)
    : []

  const pastSum = pastBucketSnaps.reduce(
    (sum, snap) => sum + (snap.data()?.count ?? 0),
    0,
  )

  // ESCRITURA (al final)
  tx.create(rollupRef, { sum_T1: pastSum + 1, ...metadata })
}

async function getSumToRemove(tx, condition, h3, lastHourISO, k) {
  const hoursToRemove = []
  const oldWindowStart = addHoursISO(
    lastHourISO,
    -(config.ROLLUP_WINDOW_HOURS - 1),
  )
  for (let j = 0; j < k; j++) {
    hoursToRemove.push(addHoursISO(oldWindowStart, j))
  }
  if (hoursToRemove.length === 0) return 0

  const bucketRefs = hoursToRemove.map((h) =>
    db.doc(paths.bucket1hDoc(condition, h3, h)),
  )

  // LECTURAS
  const bucketSnaps = await tx.getAll(...bucketRefs)
  return bucketSnaps.reduce((sum, snap) => sum + (snap.data()?.count ?? 0), 0)
}

/**
 * [TX-2] Ejecuta la lógica compleja del rollup en su PROPIA transacción.
 */
async function runRollupUpdateTransaction(condition, h3, eventHourISO, now) {
  const rollupRef = db.doc(paths.rollup1hDoc(condition, h3))

  return db.runTransaction(async (tx) => {
    // --- LECTURAS (TODAS AL INICIO DE TX-2) ---
    const rollupSnap = await tx.get(rollupRef)
    const metadata = { last_updated_at: now, last_bucket_1hISO: eventHourISO }

    if (!rollupSnap.exists) {
      // Caso 1: Inicializar. Esta función interna hace sus propias lecturas.
      await initializeRollup(
        tx,
        rollupRef,
        condition,
        h3,
        eventHourISO,
        metadata,
      )
      return
    }

    const data = rollupSnap.data()
    const last = data.last_bucket_1hISO

    // --- LÓGICA (SIN LECTURAS NI ESCRITURAS) ---
    if (eventHourISO === last) {
      // Caso 2: Misma hora. Solo escribirá.
      tx.update(rollupRef, {
        sum_T1: FieldValue.increment(1),
        last_updated_at: now,
      })
    } else if (eventHourISO > last) {
      // Caso 3: Avanza la ventana. Necesita leer los buckets que salen.
      const k = hoursBetween(last, eventHourISO)

      // LECTURA (ocurre antes de la escritura de abajo)
      const sumToRemove = await getSumToRemove(tx, condition, h3, last, k)

      // ESCRITURA (al final)
      tx.update(rollupRef, {
        sum_T1: FieldValue.increment(1 - sumToRemove),
        ...metadata,
      })
    } else {
      // Caso 4: Evento tardío.
      const windowStart = addHoursISO(last, -(config.ROLLUP_WINDOW_HOURS - 1))
      if (eventHourISO >= windowStart) {
        // Cae dentro. Solo escribe.
        tx.update(rollupRef, {
          sum_T1: FieldValue.increment(1),
          last_updated_at: now,
        })
      } else {
        // Cae fuera. Solo escribe 'updated_at'.
        tx.update(rollupRef, { last_updated_at: now })
      }
    }
  })
}

// --- Lógica Post-Transacción ---

async function calculateDensity(condition, neighborsH3) {
  // ... (sin cambios)
  if (!neighborsH3 || neighborsH3.length === 0) return 0
  const rollupRefs = neighborsH3.map((h3) =>
    db.doc(paths.rollup1hDoc(condition, h3)),
  )
  const rollupSnaps = await db.getAll(...rollupRefs)
  return rollupSnaps.reduce((sum, snap) => sum + (snap.data()?.sum_T1 ?? 0), 0)
}

async function enqueueDbscanJob(
  condition,
  h3Center,
  neighborsH3,
  density_T1,
  now,
) {
  console.log('enqueue dbscan job started')
  const hourKey = getHourKey(now)
  const winAnchorISO = floorToHourISO(now)
  const clusterId = paths.clusterId(condition, h3Center, hourKey)
  const alertRef = db.doc(paths.alertDoc(clusterId))
  let shouldEnqueue = false
  try {
    await db.runTransaction(async (tx) => {
      console.log('tx started')
      const alertSnap = await tx.get(alertRef)
      if (alertSnap.exists && alertSnap.data().job_status === 'enqueued') {
        tx.update(alertRef, { density_T1: density_T1, last_seen_at: now })
        shouldEnqueue = false
        return
      }
      const alertData = {
        cluster_id: clusterId,
        condition: condition,
        h3_center: h3Center,
        win_anchor: winAnchorISO,
        state: 'pre_alerta',
        job_status: 'enqueued',
        neighbors: neighborsH3,
        density_T1: density_T1,
        minPts_H3: config.MIN_PTS_H3,
        first_seen_at: alertSnap.exists ? alertSnap.data().first_seen_at : now,
        last_seen_at: now,
      }
      tx.set(alertRef, alertData, { merge: true })
      shouldEnqueue = true
      console.log('tx finished')
    })
    if (shouldEnqueue) {
      console.log('should enqueue', shouldEnqueue)
      const sinceUTC = DateTime.fromJSDate(now)
        .minus({ hours: config.ROLLUP_WINDOW_HOURS })
        .toISO()
      const payload = { clusterId, condition, h3Center, neighborsH3, sinceUTC }
      await taskQueue.enqueue(payload)
      logger.info(`DBSCAN job enqueued for cluster ${clusterId}`, payload)
    }
  } catch (error) {
    logger.error(`Failed to enqueue job for cluster ${clusterId}`, { error })
  }
}

// --- Función Principal 'onCall' ---

export const ingestCase = onCall(async (request) => {
  if (!request.auth) {
    throw new HttpsError(
      'unauthenticated',
      'La función debe ser llamada estando autenticado.',
    )
  }
  const { event_id, event_time_utc, lat, lng, condition } = request.data
  if (!event_id || !event_time_utc || !lat || !lng || !condition) {
    throw new HttpsError('invalid-argument', 'Payload incompleto.')
  }

  try {
    // 1. Cálculos Geo/Tiempo
    const eventTime = new Date(event_time_utc)
    const now = new Date()
    const hourISO = floorToHourISO(eventTime)
    const h3 = toH3(lat, lng, config.H3_RES)
    const geohash = toGeohash(lat, lng, config.GEOHASH_PRECISION)

    // 2. Transacción 1: Ingesta, Idempotencia y Bucket
    let idempotencyHit = false
    console.log('ingestCase | tx started')
    await db.runTransaction(async (tx) => {
      // --- LECTURAS (AL INICIO DE TX-1) ---
      const dedupRef = db.doc(paths.dedupDoc(event_id))
      const dedupSnap = await tx.get(dedupRef)

      // --- LÓGICA ---
      if (dedupSnap.exists) {
        idempotencyHit = true
        return // Salir de la transacción
      }

      // --- ESCRITURAS (AL FINAL DE TX-1) ---
      const expireAt = DateTime.fromJSDate(now)
        .plus({ hours: config.IDEMPOTENCY_TTL_HOURS })
        .toJSDate()
      tx.create(dedupRef, { created_at: now, expire_at: expireAt })

      const caseRef = db.doc(paths.caseDoc(event_id))
      tx.create(caseRef, {
        ...request.data,
        location: new GeoPoint(lat, lng),
        h3: h3,
        geohash: geohash,
        created_at: eventTime,
        ingested_at: now,
      })

      const bucketRef = db.doc(paths.bucket1hDoc(condition, h3, hourISO))
      tx.set(bucketRef, { count: FieldValue.increment(1) }, { merge: true })
    }) // Fin de la Transacción 1
    console.log('ingestCase | tx finished')
    if (idempotencyHit) {
      return { ok: true, status: 'duplicate_skipped', event_id }
    }

    // 3. Transacción 2: Actualización del Rollup
    // (Se ejecuta solo si la ingesta fue exitosa y no fue duplicada)
    await runRollupUpdateTransaction(condition, h3, hourISO, now)

    // 4. Post-Transacción: Cálculo de Densidad y Encolado de Job
    const neighborsH3 = centerWithKRing1(h3)
    const density_T1 = await calculateDensity(condition, neighborsH3)

    let alertTriggered = false
    if (density_T1 >= config.MIN_PTS_H3) {
      logger.info(
        `H3 density threshold met for ${condition}|${h3} (${density_T1} >= ${config.MIN_PTS_H3})`,
      )
      console.log('ingestCase | enqueueing job')
      await enqueueDbscanJob(condition, h3, neighborsH3, density_T1, now)
      console.log('ingestCase | job enqueued')
      alertTriggered = true
      console.log('ingestCase | alert triggered')
    }

    // 5. Respuesta Exitosa
    return {
      ok: true,
      event_id,
      h3,
      density_T1,
      alert_triggered: alertTriggered,
    }
  } catch (error) {
    logger.error('Error fatal en ingestCase', {
      error,
      data: request.data,
      stack: error.stack,
    })
    throw new HttpsError('internal', 'Error interno al procesar el caso.')
  }
})
