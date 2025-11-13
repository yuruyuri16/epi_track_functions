/**
 * src/functions/httpIngestCase.js
 * * Función 'onRequest' (Etapa 1) - Implementación final para HTTP
 */

// Importaciones
import { onRequest } from 'firebase-functions/v2/https' // CAMBIO: Usar onRequest
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
      // Caso 1: Inicializar.
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

    // --- LÓGICA ---
    if (eventHourISO === last) {
      // Caso 2: Misma hora.
      tx.update(rollupRef, {
        sum_T1: FieldValue.increment(1),
        last_updated_at: now,
      })
    } else if (eventHourISO > last) {
      // Caso 3: Avanza la ventana.
      const k = hoursBetween(last, eventHourISO)

      // LECTURA
      const sumToRemove = await getSumToRemove(tx, condition, h3, last, k)

      // ESCRITURA
      tx.update(rollupRef, {
        sum_T1: FieldValue.increment(1 - sumToRemove),
        ...metadata,
      })
    } else {
      // Caso 4: Evento tardío.
      const windowStart = addHoursISO(last, -(config.ROLLUP_WINDOW_HOURS - 1))
      if (eventHourISO >= windowStart) {
        // Cae dentro.
        tx.update(rollupRef, {
          sum_T1: FieldValue.increment(1),
          last_updated_at: now,
        })
      } else {
        // Cae fuera.
        tx.update(rollupRef, { last_updated_at: now })
      }
    }
  })
}

// --- Lógica Post-Transacción ---

async function calculateDensity(condition, neighborsH3) {
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
  console.log('dbscan job started')
  logger.info('dbscan job started')
  const hourKey = getHourKey(now)
  const winAnchorISO = floorToHourISO(now)
  const clusterId = paths.clusterId(condition, h3Center, hourKey)
  const alertRef = db.doc(paths.alertDoc(clusterId))
  let shouldEnqueue = false
  try {
    await db.runTransaction(async (tx) => {
      const alertSnap = await tx.get(alertRef)
      if (alertSnap.exists && alertSnap.data().job_status === 'enqueued') {
        tx.update(alertRef, { density_T1: density_T1, last_seen_at: now })
        shouldEnqueue = false
        console.log('dbscan job already enqueued')
        logger.info('dbscan job already enqueued')
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
    })
    console.log('should enqueue', shouldEnqueue)
    logger.info('should enqueue', shouldEnqueue)
    if (shouldEnqueue) {
      const sinceUTC = DateTime.fromJSDate(now)
        .minus({ hours: config.ROLLUP_WINDOW_HOURS })
        .toISO()
      const payload = { clusterId, condition, h3Center, neighborsH3, sinceUTC }
      console.log('enqueuing dbscan job')
      logger.info('enqueuing dbscan job')
      await taskQueue.enqueue(payload)
      logger.info(`DBSCAN job enqueued for cluster ${clusterId}`, payload)
    }
  } catch (error) {
    logger.error(`Failed to enqueue job for cluster ${clusterId}`, { error })
  }
}

// --- Función Principal 'onRequest' ---

export const ingestCaseHttp = onRequest(
  {
    maxInstances: 5,
  },
  async (req, res) => {
    // CAMBIO: Nombre y firma (req, res)
    // 1. Validar Método HTTP (Opcional, pero bueno para ingesta)
    if (req.method !== 'POST') {
      return res.status(405).send({
        ok: false,
        status: 'error',
        message: 'Method Not Allowed. Use POST.',
      })
    }

    // 2. Acceso y Validación del Payload
    const { event_id, event_time_utc, lat, lng, condition } = req.body // CAMBIO: Usar req.body

    if (!event_id || !event_time_utc || !lat || !lng || !condition) {
      logger.error('Payload incompleto', { payload: req.body })
      // CAMBIO: Enviar respuesta HTTP 400 (Bad Request)
      return res.status(400).send({
        ok: false,
        status: 'error',
        message:
          'Payload incompleto. Faltan event_id, event_time_utc, lat, lng o condition.',
      })
    }

    try {
      // 3. Cálculos Geo/Tiempo
      const eventTime = new Date(event_time_utc)
      const now = new Date()
      const hourISO = floorToHourISO(eventTime)
      const h3 = toH3(lat, lng, config.H3_RES)
      const geohash = toGeohash(lat, lng, config.GEOHASH_PRECISION)

      // 4. Transacción 1: Ingesta, Idempotencia y Bucket
      let idempotencyHit = false
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
          ...req.body, // CAMBIO: Usar req.body
          location: new GeoPoint(lat, lng),
          h3: h3,
          geohash: geohash,
          created_at: eventTime,
          ingested_at: now,
        })

        const bucketRef = db.doc(paths.bucket1hDoc(condition, h3, hourISO))
        tx.set(bucketRef, { count: FieldValue.increment(1) }, { merge: true })
      }) // Fin de la Transacción 1

      if (idempotencyHit) {
        // CAMBIO: Respuesta HTTP 202 (Accepted) para duplicados
        return res
          .status(202)
          .send({ ok: true, status: 'duplicate_skipped', event_id })
      }

      // 5. Transacción 2: Actualización del Rollup
      await runRollupUpdateTransaction(condition, h3, hourISO, now)

      // 6. Post-Transacción: Cálculo de Densidad y Encolado de Job
      const neighborsH3 = centerWithKRing1(h3)
      const density_T1 = await calculateDensity(condition, neighborsH3)

      let alertTriggered = false
      if (density_T1 >= config.MIN_PTS_H3) {
        logger.info(
          `H3 density threshold met for ${condition}|${h3} (${density_T1} >= ${config.MIN_PTS_H3})`,
        )
        console.log('enqueueing dbscan job')
        logger.info('enqueueing dbscan job')
        await enqueueDbscanJob(condition, h3, neighborsH3, density_T1, now)
        alertTriggered = true
      }

      // 7. Respuesta Exitosa
      // CAMBIO: Enviar respuesta HTTP 201 (Created)
      return res.status(201).send({
        ok: true,
        event_id,
        h3,
        density_T1,
        alert_triggered: alertTriggered,
        message: 'Case ingested and processed successfully.',
      })
    } catch (error) {
      logger.error('Error fatal en ingestCaseHttp', {
        error,
        data: req.body,
        stack: error.stack,
      })
      // CAMBIO: Enviar respuesta HTTP 500 (Internal Server Error)
      return res.status(500).send({
        ok: false,
        status: 'error',
        message: 'Error interno al procesar el caso.',
      })
    }
  },
)
