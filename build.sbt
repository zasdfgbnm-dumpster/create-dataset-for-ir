name := "CreateDataset"

version := "0.0.1-SNAPSHOT"

mainClass in Compile := Some("irms.Main")

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"
