import sbt.util

name := "rishabh_mehta_hw2"

version := "0.1"

scalaVersion := "2.13.3"

logLevel := Level.Debug

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.0"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.0"


// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.2.1"

// https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
//libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

// https://mvnrepository.com/artifact/org.scala-lang.modules/scala-xml
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M2"

// https://mvnrepository.com/artifact/org.slf4j/slf4j-log4j12
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.2" % Test

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}