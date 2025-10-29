import { DateTime } from 'luxon'

/** Trunca a la hora ISO: 2025-10-28T18:00:00.000Z */
export function floorToHourISO(date) {
  return DateTime.fromJSDate(date).toUTC().startOf('hour').toISO()
}

/** Genera la clave de hora: 2025-10-28T18 */
export function getHourKey(date) {
  return DateTime.fromJSDate(date)
    .toUTC()
    .startOf('hour')
    .toFormat("yyyy-MM-dd'T'HH")
}

/** Suma 'n' horas a un string ISO */
export function addHoursISO(hourISO, n) {
  return DateTime.fromISO(hourISO, { zone: 'utc' }).plus({ hours: n }).toISO()
}

/** Calcula la diferencia en horas (truncado) */
export function hoursBetween(aISO, bISO) {
  const start = DateTime.fromISO(aISO, { zone: 'utc' })
  const end = DateTime.fromISO(bISO, { zone: 'utc' })
  return end.diff(start, 'hours').hours
}
