const SEP = '|' // Separador para IDs compuestos

export const paths = {
  caseDoc: (eventId) => `cases/${eventId}`,

  dedupDoc: (eventId) => `dedup/${eventId}`,

  bucket1hDoc: (condition, h3, hourISO) =>
    `buckets_1h/${condition}${SEP}${h3}${SEP}${hourISO}`,

  rollup1hDoc: (condition, h3) => `rollups_1h/${condition}${SEP}${h3}`,

  /**
   * Documento de Alerta/Clúster.
   * El ID se basa en la hora de ANCLAJE (win_anchor), no en la hora del evento.
   */
  alertDoc: (clusterId) => `alerts/${clusterId}`,

  /**
   * ID del clúster: {cond|h3Center|YYYY-MM-DDTHH}
   */
  clusterId: (condition, h3Center, hourKey) =>
    `${condition}${SEP}${h3Center}${SEP}${hourKey}`,
}
