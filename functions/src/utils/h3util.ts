import * as h3 from "h3-js";

/** H3 cell index at a given resolution. */
export function toH3(lat: number, lng: number, res: number): string {
  return h3.latLngToCell(lat, lng, res);
}

/** Returns center + neighbors (k=1) unique; center first. */
export function centerWithKRing1(center: string): string[] {
  const disk = h3.gridDisk(center, 1); // includes center
  const set = new Set<string>(disk);
  if (!set.has(center)) set.add(center);
  return [center, ...Array.from(set).filter((c) => c !== center)];
}