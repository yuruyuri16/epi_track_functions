export const config = {
  // --- Configuración de Detección ---

  // Resolución H3. 8 es ~0.74 km² por hexágono.
  H3_RES: 8,

  // (minPts) Umbral de densidad H3 para disparar una pre-alerta.
  MIN_PTS_H3: 12,

  // (minPts) Puntos mínimos para DBSCAN para formar un clúster.
  MIN_PTS_DBSCAN: 5,

  // (epsilon) Distancia máxima en KM para que DBSCAN considere puntos como vecinos.
  EPSILON_KM: 1.0,

  // Precisión del Geohash a almacenar
  GEOHASH_PRECISION: 7,

  // --- Configuración de Ventana y TTL ---

  // Ventana deslizante para los rollups (72 horas)
  ROLLUP_WINDOW_HOURS: 72,

  // TTL para el ancla de idempotencia (96 horas)
  IDEMPOTENCY_TTL_HOURS: 96,

  // TTL para los casos crudos (90 días)
  CASES_TTL_DAYS: 90,

  // Límite de seguridad para el worker DBSCAN
  DBSCAN_QUERY_LIMIT: 1500,
}
