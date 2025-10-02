import { DateTime } from 'luxon';

/** Floors a Date to the hour in UTC, returns ISO like 2025-09-24T14:00:00Z */
export function floorToHourISO(d: Date): string {
  return DateTime.fromJSDate(d, { zone: 'utc' }).startOf('hour').toISO({ suppressMilliseconds: true })!;
}

/** Adds hours (can be negative) to an hour-anchored ISO string in UTC. */
export function addHoursISO(hourISO: string, hours: number): string {
  return DateTime.fromISO(hourISO, { zone: 'utc' }).plus({ hours }).toISO({ suppressMilliseconds: true })!;
}

/** Whole-hour difference: b - a */
export function hoursBetween(aISO: string, bISO: string): number {
  const a = DateTime.fromISO(aISO, { zone: 'utc' });
  const b = DateTime.fromISO(bISO, { zone: 'utc' });
  return Math.floor(b.diff(a, 'hours').hours);
}
