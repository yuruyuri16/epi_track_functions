import admin from 'firebase-admin'
import { getFunctions } from 'firebase-admin/functions'

admin.initializeApp()

// Base de datos de Firestore
const db = admin.firestore()
db.settings({
  ignoreUndefinedProperties: true,
})

// Cola de Tareas para el worker DBSCAN (Etapa 2)
// El nombre 'processDbscanJob' debe coincidir con el nombre de la función exportada.
const taskQueue = getFunctions().taskQueue('processDbscanJob')

export { admin, db, taskQueue }
