import { db } from "./bootstrap.js";

export interface RuntimeConfig {
  // Global (not region-based)
  h3_res?: number;   // default 8
  minPts?: number;   // default 12

  // Kept for future modules (unused here)
  max_points_per_job?: number;
}

let _cached: RuntimeConfig | null = null;

/** Load operational parameters from Firestore: /config/runtime (cached). */
export async function loadRuntime(): Promise<RuntimeConfig> {
  if (_cached) return _cached;
  const snap = await db.doc("config/runtime").get();
  _cached = (snap.data() || {}) as RuntimeConfig;
  return _cached;
}

export function getH3Res(cfg: RuntimeConfig): number {
  return cfg.h3_res ?? 8;
}

export function getMinPts(cfg: RuntimeConfig): number {
  return cfg.minPts ?? 12;
}