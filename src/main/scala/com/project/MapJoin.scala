package com.project

class MapJoin {

  def join(tp: List[Trip], rt: List[Route]): List[TripRoute] = {
    // mapping on route
    val routeMap: Map[Int, Route] = rt.map(route => route.route_id.toInt -> route).toMap

    tp.filter(trip => routeMap.contains(trip.route_id.toInt)).map(trip => TripRoute(trip, (routeMap(trip.route_id.toInt))))
  }

}