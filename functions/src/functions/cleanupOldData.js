/**
 * * Función Programada (diaria)
 * 1. Elimina anclas de idempotencia ('dedup') expiradas (TTL).
 * 2. Elimina casos crudos ('cases') antiguos.
 */

import { onSchedule } from 'firebase-functions/v2/scheduler'
import logger from 'firebase-functions/logger'
import { DateTime } from 'luxon'

import { db } from '../firebase.js'
import { config } from '../config.js'

/**
 * Utiliza un BulkWriter para eliminar documentos de una query
 */
async function deleteQueryBatch(query) {
  const bulkWriter = db.bulkWriter()
  let count = 0

  try {
    const snapshot = await query.get()

    if (snapshot.empty) {
      return 0
    }

    snapshot.docs.forEach((doc) => {
      bulkWriter.delete(doc.ref)
      count++
    })

    await bulkWriter.close()
    return count
  } catch (error) {
    logger.error('Error durante la eliminación batch', { error })
    if (bulkWriter) await bulkWriter.close() // Intenta cerrar si falla
    throw error
  }
}

// --- Función Principal 'onSchedule' ---
// Se ejecuta todos los días a las 4:00 AM
export const cleanupOldData = onSchedule('0 4 * * *', async () => {
  logger.info('Iniciando tarea de limpieza de datos antiguos...')
  const now = new Date()

  // 1. Limpiar 'dedup' (Anclas de Idempotencia)
  try {
    const dedupQuery = db
      .collection('dedup')
      .where('expire_at', '<', now)
      .limit(1000) // Limite por ejecución para evitar timeouts

    const dedupCount = await deleteQueryBatch(dedupQuery)
    logger.info(`Limpieza: ${dedupCount} documentos 'dedup' eliminados.`)
  } catch (error) {
    logger.error('Falló la limpieza de "dedup"', { error })
  }

  // 2. Limpiar 'cases' (Casos Crudos)
  try {
    const oldCasesTimestamp = DateTime.fromJSDate(now)
      .minus({ days: config.CASES_TTL_DAYS })
      .toJSDate()

    const casesQuery = db
      .collection('cases')
      .where('created_at', '<', oldCasesTimestamp)
      .limit(1000) // Limite por ejecución

    const casesCount = await deleteQueryBatch(casesQuery)
    logger.info(
      `Limpieza: ${casesCount} documentos 'cases' antiguos eliminados.`,
    )
  } catch (error) {
    logger.error('Falló la limpieza de "cases"', { error })
  }

  logger.info('Tarea de limpieza de datos finalizada.')
})
