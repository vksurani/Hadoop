name := "FinalProject"

version := "0.1"

scalaVersion := "2.13.2"

val hadoopVersion = "2.7.7"

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdn5.16.2"

resolvers += "cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion