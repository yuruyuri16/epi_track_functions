/** Centralized path builders to avoid string typos. */
export const paths = {
    caseDoc: (eventId: string) => `cases/${eventId}`,
    dedupDoc: (eventId: string) => `dedup/${eventId}`,
    bucket1hDoc: (cond: string, h3: string, hourISO: string) =>
      `buckets_1h/${cond}|${h3}|${hourISO}`,
    rollup1hDoc: (cond: string, h3: string) => `rollups_1h/${cond}|${h3}`,
    alertDoc: (clusterId: string) => `alerts/${clusterId}`
  };
  
  /** Cluster key: cond|h3_center|YYYY-MM-DDTHH */
  export function clusterId(cond: string, h3Center: string, hourISO: string): string {
    return `${cond}|${h3Center}|${hourISO.substring(0, 13)}`;
  }