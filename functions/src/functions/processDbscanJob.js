/**
 * * Worker de Task Queue (Etapa 2) - ¡AHORA USANDO TURF.JS!
 * 1. Recibe el payload del job.
 * 2. Marca la alerta como 'processing'.
 * 3. Consulta los 'cases' crudos y los formatea como GeoJSON FeatureCollection.
 * 4. Ejecuta Turf DBSCAN.
 * 5. Actualiza la alerta a 'confirmed' o 'rejected'.
 */

import { onTaskDispatched } from 'firebase-functions/v2/tasks'
import logger from 'firebase-functions/logger'
import { FieldValue } from 'firebase-admin/firestore'
import { featureCollection, point as turfPoint } from '@turf/helpers'
import turfDbscan from '@turf/clusters-dbscan'

import { db } from '../firebase.js'
import { config } from '../config.js'
import { paths } from '../utils/paths.js'
// Ya no importamos haversineDistance

/**
 * Consulta los casos crudos y los devuelve como una FeatureCollection de GeoJSON.
 */
async function getPointsAsGeoJSON(condition, neighborsH3, sinceUTC) {
  const casesRef = db.collection('cases')
  const query = casesRef
    .where('condition', '==', condition)
    .where('h3', 'in', neighborsH3)
    .where('created_at', '>=', new Date(sinceUTC))
    .limit(config.DBSCAN_QUERY_LIMIT)

  const snapshot = await query.get()

  console.log('getPointsAsGeoJSON | snapshot size', snapshot.size)

  if (snapshot.empty) {
    return featureCollection([])
  }

  console.log('getPointsAsGeoJSON | mapping docs to features')
  // Mapea los documentos de Firestore a 'Features' de GeoJSON
  const features = snapshot.docs.map((doc) => {
    const data = doc.data()
    const coordinates = [data.lng, data.lat] // GeoJSON es [lng, lat]
    const properties = {
      id: doc.id,
      event_time_utc: data.event_time_utc,
      //... cualquier otra propiedad que quieras pasar
    }
    return turfPoint(coordinates, properties)
  })

  console.log('getPointsAsGeoJSON | returning feature collection')
  return featureCollection(features)
}

/**
 * Analiza los resultados de Turf DBSCAN
 */
function analyzeTurfResults(clusteredCollection) {
  const clustersMap = new Map() // Map<clusterId, caseId[]>
  let noiseCount = 0

  for (const feature of clusteredCollection.features) {
    const clusterId = feature.properties.cluster

    if (clusterId === undefined) {
      noiseCount++
    } else {
      // Agrupar puntos por su ID de clúster
      if (!clustersMap.has(clusterId)) {
        clustersMap.set(clusterId, [])
      }
      clustersMap.get(clusterId).push(feature.properties.id) // Guardamos el ID del caso
    }
  }

  return {
    clustersMap: clustersMap,
    noiseCount: noiseCount,
    totalPoints: clusteredCollection.features.length,
  }
}

// --- Función Principal 'onTaskDispatched' ---

export const processDbscanJob = onTaskDispatched(async (request) => {
  console.log('processDbscanJob | started')
  const { clusterId, condition, neighborsH3, sinceUTC } = request.data
  const alertRef = db.doc(paths.alertDoc(clusterId))

  logger.info(`Processing DBSCAN job for ${clusterId} using Turf.js...`)

  try {
    // 1. Marcar el job como 'processing'
    console.log('processDbscanJob | 1 updating alert status to processing')
    await alertRef.update({
      job_status: 'processing',
      job_started_at: FieldValue.serverTimestamp(),
    })

    console.log('processDbscanJob | 2 getting points as geojson')
    // 2. Obtener los puntos como GeoJSON
    const pointsCollection = await getPointsAsGeoJSON(
      condition,
      neighborsH3,
      sinceUTC,
    )

    console.log('processDbscanJob | 3 checking points count')
    if (pointsCollection.features.length < config.MIN_PTS_DBSCAN) {
      logger.warn(
        `Job ${clusterId} abortado: Puntos insuficientes (${pointsCollection.features.length})`,
      )
      console.log('processDbscanJob | insufficient points')
      await alertRef.update({
        job_status: 'completed',
        state: 'rejected',
        confirm_label: 'insufficient_points',
        job_finished_at: FieldValue.serverTimestamp(),
      })
      console.log('processDbscanJob | finished')
      return
    }

    console.log('processDbscanJob | 3 executing dbscan')
    // 3. Ejecutar DBSCAN (¡La llamada ahora es mucho más limpia!)
    const clusteredCollection = turfDbscan(
      pointsCollection,
      config.EPSILON_KM,
      {
        minPoints: config.MIN_PTS_DBSCAN,
        units: 'kilometers', // Especificamos unidades, no más Haversine manual
      },
    )

    // 4. Analizar resultados
    console.log('processDbscanJob | 4 analyzing results')
    const { clustersMap, noiseCount, totalPoints } =
      analyzeTurfResults(clusteredCollection)
    const pointsInClusters = totalPoints - noiseCount

    console.log('processDbscanJob | checking clusters count')
    if (clustersMap.size > 0) {
      // Usamos .size en lugar de .length
      logger.info(
        `¡BROTE CONFIRMADO! Job ${clusterId} encontró ${clustersMap.size} clúster(s).`,
      )
      console.log('processDbscanJob | brote confirmado')
      // Convertimos el Map a un objeto simple, aplicando el .slice() aquí
      const clustersPreviewMap = {}
      let count = 0
      for (const [turfClusterId, caseIds] of clustersMap.entries()) {
        if (count >= 5) break // Límite de seguridad

        // Usamos la clave original de Turf (ej. '0', '1', etc.)
        clustersPreviewMap[`cluster_${turfClusterId}`] = caseIds
        count++
      }
      console.log('processDbscanJob | updating alert status to completed')
      await alertRef.update({
        job_status: 'completed',
        state: 'confirmed',
        confirm_label: 'cluster_found',
        job_finished_at: FieldValue.serverTimestamp(),
        dbscan_results: {
          cluster_count: clustersMap.length,
          points_in_clusters: pointsInClusters,
          noise_count: noiseCount,
          total_points_analyzed: totalPoints,
          clusters_preview: clustersPreviewMap,
        },
      })
      console.log('processDbscanJob | confirmed outbreak')
    } else {
      logger.info(`Falsa Alarma: Job ${clusterId} solo encontró ruido.`)
      console.log('processDbscanJob | false alarm')
      await alertRef.update({
        job_status: 'completed',
        state: 'rejected',
        confirm_label: 'noise_only',
        job_finished_at: FieldValue.serverTimestamp(),
        dbscan_results: {
          cluster_count: 0,
          points_in_clusters: 0,
          noise_count: noiseCount,
          total_points_analyzed: totalPoints,
        },
      })
      console.log('processDbscanJob | false alarm')
    }
  } catch (error) {
    console.log('processDbscanJob | error')
    console.log(error)
    logger.error(`Error fatal en processDbscanJob ${clusterId}`, {
      error,
      stack: error.stack,
    })
    try {
      await alertRef.update({
        job_status: 'failed',
        error_message: error.message,
        job_finished_at: FieldValue.serverTimestamp(),
      })
    } catch (e) {
      logger.error('No se pudo ni siquiera marcar el job como fallido.', {
        error: e,
      })
    }
  }
})
