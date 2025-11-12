/**
 * Import function triggers from their respective submodules:
 *
 * const {onCall} = require("firebase-functions/v2/https");
 * const {onDocumentWritten} = require("firebase-functions/v2/firestore");
 *
 * See a full list of supported triggers at https://firebase.google.com/docs/functions
 */

import { setGlobalOptions } from 'firebase-functions'

// import { setGlobalOptions } from 'firebase-functions'
// import { h3 } from 'h3-js'
// import { onRequest } from 'firebase-functions/https'
import logger from 'firebase-functions/logger'

// For cost control, you can set the maximum number of containers that can be
// running at the same time. This helps mitigate the impact of unexpected
// traffic spikes by instead downgrading performance. This limit is a
// per-function limit. You can override the limit for each function using the
// `maxInstances` option in the function's options, e.g.
// `onRequest({ maxInstances: 5 }, (req, res) => { ... })`.
// NOTE: setGlobalOptions does not apply to functions using the v1 API. V1
// functions should each use functions.runWith({ maxInstances: 10 }) instead.
// In the v1 API, each function can only serve one request per container, so
// this will be the maximum concurrent request count.
setGlobalOptions({ maxInstances: 10 })

// Create and deploy your first functions
// https://firebase.google.com/docs/functions/get-started

// export const helloWorld = onRequest(async (request, response) => {
//   logger.info("Hello logs!", {structuredData: true});
//   response.send("Hello from Firebase!");
// });

/**
 * * Punto de entrada principal. Exporta todas las Cloud Functions.
 */

import { ingestCase } from './functions/ingestCase.js'
import { processDbscanJob } from './functions/processDbscanJob.js'
import { cleanupOldData } from './functions/cleanupOldData.js'
import { ingestCaseHttp } from './functions/ingestCaseHttp.js'

export { ingestCase, processDbscanJob, cleanupOldData, ingestCaseHttp }
