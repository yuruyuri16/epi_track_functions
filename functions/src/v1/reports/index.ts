import { Timestamp } from 'firebase-admin/firestore'
import { onRequest } from 'firebase-functions/https'

import { latLngToCell } from 'h3-js'
import { admin, db } from '../../bootstrap'
import { addHoursISO, floorToHourISO, hoursBetween } from '../../utils/time'
import * as ngeohash from 'ngeohash'
import { paths } from '../../utils/firestorePaths'
import { DateTime } from 'luxon'

export interface IngestPayload {
  event_id: string
  event_time_utc: string // ISO
  lat: number
  lng: number
  condition: string // e.g., "ILI"
  region_code: string // ISO 3166-2
}

export interface CaseDoc {
  event_time: Date
  reported_at: Timestamp
  lat: number
  lng: number
  h3: string
  geohash: string
  cond: string
  region: string
  source?: string
}

export interface Rollup1hDoc {
  sum_T1: number
  last_bucket_1hISO: string // hour-anchored ISO UTC
  last_updated_at: Timestamp
}

/// LimaBounds with static values
// const LimaBounds = {
//   latMin: -12.1167,
//   latMax: -12.0667,
//   lngMin: -77.2,
//   lngMax: -76.62
// }

/// verify the range of lat and lng to only Lima, Peru
// if (payload.lat < LimaBounds.latMin || payload.lat > LimaBounds.latMax || payload.lng < LimaBounds.lngMin || payload.lng > LimaBounds.lngMax) {
//   res.json({
//     status: 'error',
//     message: 'The report is not in Lima, Peru'
//   });
//   return;
// }

/**
 * HTTP Cloud Function.
 *
 * @param {Request} req - Cloud Function request context.
 * @param {Response} res - Cloud Function response context.
 */
const ingestCase = onRequest(async (req, res) => {
  if (req.method !== 'POST') {
    res.status(405).send('Method Not Allowed')
    return
  }

  console.log('body', req.body)
  const payload: IngestPayload = req.body

  const missing = validatePayload(payload)
  if (missing.length) {
    res.status(400).json({
      status: 'error',
      message: 'Missing required fields: ' + missing.join(', ')
    })
    return
  }

  const nowTimestamp: Timestamp = admin.firestore.Timestamp.now()
  const eventTime = new Date(payload.event_time_utc)
  const hourISO = floorToHourISO(eventTime)

  const h3Index = latLngToCell(payload.lat, payload.lng, 8)
  const geoHash = ngeohash.encode(payload.lat, payload.lng, 7)

  await db.runTransaction(async (tx) => {
    // idempotency
    const dedupRef = db.doc(paths.dedupDoc(payload.event_id))
    const dedupSnap = await tx.get(dedupRef)
    if (dedupSnap.exists) return
    tx.set(dedupRef, {
      created_at: nowTimestamp,
      expire_at: admin.firestore.Timestamp.fromDate(DateTime.utc().plus({ hours: 96 }).toJSDate())
    })

    // /cases/{event_id}
    const caseRef = db.doc(paths.caseDoc(payload.event_id))
    tx.set(caseRef, {
      event_time: eventTime,
      reported_at: nowTimestamp,
      lat: payload.lat,
      lng: payload.lng,
      h3: h3Index,
      geohash: geoHash,
      cond: payload.condition,
      region: payload.region_code,
      source: 'clinic_app_v1'
    })

    // /buckets_1h/{cond|h3|hourISO}
    const bucketRef = db.doc(paths.bucket1hDoc(payload.condition, h3Index, hourISO))
    const bucketSnap = await tx.get(bucketRef)
    const prev = bucketSnap.exists ? (bucketSnap.data()!.count as number) : 0
    const next = prev + 1
    tx.set(bucketRef, { count: next, updated_at: nowTimestamp }, { merge: true })

    // /rollups_1h/{cond|h3} - 72h sliding window
    const rollRef = db.doc(paths.rollup1hDoc(payload.condition, h3Index))
    const rollSnap = await tx.get(rollRef)
    const WINDOW = 72

    // rollup does not exist
    if (!rollSnap.exists) {
      // Seed from last 72 hour buckets (including current hour)
      let sum = 0
      for (let i = 71; i >= 0; i--) {
        const tISO = addHoursISO(hourISO, -i)
        const ref = db.doc(paths.bucket1hDoc(payload.condition!, h3Index, tISO))
        const s = await tx.get(ref)
        if (s.exists) {
          sum += (s.data()!.count as number) || 0
        } else if (tISO === hourISO) {
          sum += next // we just wrote this hour's bucket in this tx
        }
      }
      const doc: Rollup1hDoc = {
        sum_T1: sum,
        last_bucket_1hISO: hourISO,
        last_updated_at: nowTimestamp
      }
      tx.set(rollRef, doc, { merge: false })
      return
    }

    // rollup exists
    const data = rollSnap.data() as Rollup1hDoc
    let sum = data.sum_T1 || 0
    const last = data.last_bucket_1hISO || hourISO

    if (hourISO === last) {
      sum = sum + 1 // case falls into current anchor hour
      tx.set(rollRef, { sum_T1: sum, last_bucket_1hISO: last, last_updated_at: nowTimestamp }, { merge: true })
    } else if (hourISO > last) {
      // time advanced k hours: drop outgoing tail buckets and add +1
      const k = Math.max(1, hoursBetween(last, hourISO))
      let outSum = 0
      for (let j = 0; j < k; j++) {
        const outISO = addHoursISO(hourISO, -WINDOW - j)
        const outRef = db.doc(paths.bucket1hDoc(payload.condition!, h3Index, outISO))
        const outSnap = await tx.get(outRef)
        if (outSnap.exists) outSum += (outSnap.data()!.count as number) || 0
      }
      sum = sum + 1 - outSum
      tx.set(rollRef, { sum_T1: sum, last_bucket_1hISO: hourISO, last_updated_at: nowTimestamp }, { merge: true })
    } else {
      // tardy case: include if within [last-71h, last]
      const lower = addHoursISO(last, -71)
      if (hourISO >= lower) {
        sum = sum + 1
        tx.set(rollRef, { sum_T1: sum, last_bucket_1hISO: last, last_updated_at: nowTimestamp }, { merge: true })
      }
    }
  })
})

function validatePayload(p: Partial<IngestPayload>): string[] {
  const missing: string[] = []
  if (!p?.event_id) missing.push('event_id')
  if (!p?.event_time_utc) missing.push('event_time_utc')
  if (typeof p?.lat !== 'number') missing.push('lat')
  if (typeof p?.lng !== 'number') missing.push('lng')
  if (!p?.condition) missing.push('condition')
  if (!p?.region_code) missing.push('region_code')
  return missing
}

exports.ingestCase = ingestCase
