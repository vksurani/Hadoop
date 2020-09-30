package com.project

class MapEnrichedJoin {

  def join(tp: List[TripRoute], rt: List[Calender]): List[EnrichedTrip] = {
    // mapping on calendar
    val calMap: Map[String, Calender] = rt.map(calendar => calendar.service_id -> calendar).toMap

    // Join route and trip and return the result
    tp.filter(triproute => calMap.contains(triproute.trip.service_id)).map(triproute => EnrichedTrip(triproute, calMap(triproute.trip.service_id)))
  }
}
//  val resFile = new BufferedWriter(new FileWriter("output1.csv"))

