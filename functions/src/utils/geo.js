/**
 * * Utilidades geoespaciales: H3 y ngeohash.
 * * La distancia Haversine ya no es necesaria, Turf la maneja.
 */
import { latLngToCell, gridDisk } from 'h3-js'
import ngeohash from 'ngeohash'

export function toH3(lat, lng, res) {
  return latLngToCell(lat, lng, res)
}

export function toGeohash(lat, lng, precision) {
  return ngeohash.encode(lat, lng, precision)
}

export function centerWithKRing1(centerH3) {
  return gridDisk(centerH3, 1)
}
