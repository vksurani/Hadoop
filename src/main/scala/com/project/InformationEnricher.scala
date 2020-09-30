package com.project

import java.io.OutputStreamWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}


object InformationEnricher extends App {

  val conf = new Configuration()
  conf.addResource(new Path("/Users/vasusurani/opt/hadoop-2.7.7/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/Users/vasusurani/opt/hadoop-2.7.7/etc/cloudera/hdfs-site.xml"))

  val fs: FileSystem = FileSystem.get(conf)

  fs
    .listStatus(new Path("hdfs://quickstart.cloudera:8020/user/winter2020/vasu/stm/"))
    .map(_.getPath)

  // Setting up the path to read files from HADOOP cluster
  val routespath = new Path("/user/winter2020/vasu/stm/routes.csv")
  val calendarpath = new Path("/user/winter2020/vasu/stm/calendar.csv")
  val tripspath = new Path("/user/winter2020/vasu/stm/trips.csv")

  // Opening each file with using path
  val routeStream = fs.open(routespath)
  val calendarStream = fs.open(calendarpath)
  val tripStream = fs.open(tripspath)

  // Reading files From cluster using Stream
  def readLinesFromRoute = Stream.cons(routeStream.readLine, Stream.continually(routeStream.readLine))
  def readLinesFromTrip = Stream.cons(tripStream.readLine, Stream.continually(tripStream.readLine))
  def readLinesFromCalendar = Stream.cons(calendarStream.readLine, Stream.continually(calendarStream.readLine))

  // Getting data from routes.csv
  val routeData=readLinesFromRoute.takeWhile(_ != null).toList.tail.map(_.split(",", -1))
    .map(info => Route(info(0), info(1), info(2), info(3), info(4), info(5), info(6), info(7)));

  // Getting data from trips.csv
  val tripData=readLinesFromTrip.takeWhile(_ != null).toList.tail.map(_.split(",", -1))
    .map(info => Trip(info(0), info(1), info(2), info(3), info(4).toInt, info(5).toInt, info(6).toInt,
      if (info(7).isEmpty) None else Some(info(7)), if (info(8).isEmpty) None else Some(info(8))));

  // Getting data from calendar.csv
  val calendarData=readLinesFromCalendar.takeWhile(_ != null).toList.tail.map(_.split(",", -1))
    .map(info => Calender(info(0), info(1).toInt, info(2).toInt, info(3).toInt, info(4).toInt, info(5).toInt,
      info(6).toInt, info(7).toInt, info(8), info(9)));

  // join of Trip and Route
  val tripRoutes: List[TripRoute] = new MapJoin().join(tripData, routeData)

  // Enriched join of TripRoute and Calender
  val enrichedTripRoutes: List[EnrichedTrip] = new MapEnrichedJoin().join(tripRoutes, calendarData)

  // Lookup objects for calendar and route lookup
  val calLookUp = new CalendarLookup(calendarData)
  val routeLookup = new RouteLookup(routeData)

  // Making directory(if not already exists) to store the output file
  fs.mkdirs(new Path("/user/winter2020/vasu/course3"))

  // Setting path to store output file
  val filePath = new Path("/user/winter2020/vasu/course3/output.csv")

  // Creating and writting output file with OutputStreamWriter to
  val resFile: OutputStreamWriter = new OutputStreamWriter(fs.create(filePath))

  resFile.write("service_id,monday,tuesday,wednesday,thursday,friday," +
                "saturday,sunday,start_date,end_date,trip_id,trip_headsign,direction_id," +
                "shape_id,wheelchair_accessible,note_fr,note_en,route_id,agency_id,route_short_name," +
                "route_long_name,routr_type,route_url,route-color,route_text_color"+"\n")

  for(i <- enrichedTripRoutes){
      val serviceId = i.calender.service_id
      val k = calLookUp.lookupCal(serviceId)
      resFile.write(k.service_id + "," + k.monday + "," + k.tuesday + "," + k.wednesday + "," + k.thursday + "," +
          k.friday + "," + k.saturday + "," + k.sunday + "," + k.start_date + "," + k.end_date + ",")

    for(tr <- List(i.triproute)) {
      for(k <- List(tr.trip)) {
        resFile.write(k.trip_id + "," + k.trip_headsign + "," + k.direction_id + "," + k.shape_id + "," +
          k.wheelchair_accessible + "," + k.note_fr + "," + k.note_en + ",")

        val routeId = k.route_id
        val l = routeLookup.lookupRoute(routeId)
        resFile.write(l.route_id + "," + l.agency_id + "," + l.route_short_name + "," + l.route_long_name + "," +
          l.route_type + "," + l.route_url + "," + l.route_color + "," + l.route_text_color)
      }
    }
    resFile.write("\n")
  }
  resFile.close()
}


