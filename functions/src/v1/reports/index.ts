import { onRequest } from 'firebase-functions/https';

import { latLngToCell } from 'h3-js';

/// typescript model
interface Report {
  id: string;
  name: string;
  description: string;
  lat: number;
  lng: number;
}

/// LimaBounds with static values
const LimaBounds = {
  latMin: -12.1167,
  latMax: -12.0667,
  lngMin: -77.2,
  lngMax: -76.62,
}

/// create failure response interface

/**
 * HTTP Cloud Function.
 * 
 * 
 */
const ingestCase = onRequest(async (request, response) => {
  const report: Report = request.body;

  /// verify the range of lat and lng to only Lima, Peru
  if (report.lat < LimaBounds.latMin || report.lat > LimaBounds.latMax || report.lng < LimaBounds.lngMin || report.lng > LimaBounds.lngMax) {
    response.json({
      status: 'error',
      message: 'The report is not in Lima, Peru'
    });
    return;
  }

  const cell = latLngToCell(report.lat, report.lng, 12);
  console.log(cell);

  response.json({
    status: 'success'
  });
});

exports.ingestCase = ingestCase;
