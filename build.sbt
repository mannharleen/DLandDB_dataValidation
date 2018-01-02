name := "DLandDB_dataValidation"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0" // % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"  //% "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" // % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.2.0" // % "provided"
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"