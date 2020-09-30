package com.project

class TripRouteLookup(enrich: List[EnrichedTrip]) {
  private val lookupTripTable: Map[String, EnrichedTrip] =
    enrich.map(data => data.triproute.trip.trip_id -> data).toMap
  def lookupTripId(tripId: String): EnrichedTrip = lookupTripTable.getOrElse(tripId, null)
}

case class RouteLookup(routes: List[Route]){
  private val lookupTable: Map[String, Route] = routes.map(route => route.route_id -> route).toMap
  def lookupRoute(routeId: String): Route = lookupTable.getOrElse(routeId, null)
}

case class CalendarLookup (calendars: List[Calender]){
  private val lookupTable: Map[String, Calender] = calendars.map(calendar => calendar.service_id -> calendar).toMap
  def lookupCal(serviceId: String): Calender = lookupTable.getOrElse(serviceId, null)
}

