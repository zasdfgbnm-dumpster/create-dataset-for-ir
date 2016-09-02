name := "CreateDataset"
version := "0.0.1-SNAPSHOT"
mainClass in Compile := Some("irms.Main")

scalaVersion := "2.11.8"
scalacOptions ++= Seq("-Xlint","-feature")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"

cleanFiles <+= baseDirectory { base => base / "spark-warehouse" }

//paths
//TODO: dedup path below
val workspace = System.getProperty("user.home")+"/MEGA"
val bin = workspace + "/bin"
val tables = workspace + "/tables"

//tasks

lazy val rmtables = taskKey[Unit]("remove generated tables")
rmtables := {
	"find "+tables+" -maxdepth 1 -mindepth 1 -exec rm -rf {} ;" !;
}

lazy val spark = taskKey[Unit]("run jar on spark")
spark := {
	val jar = (Keys.`package` in Compile).value
	val fullname = jar.getAbsolutePath()
	"./src/main/sh/create-links.sh" !;
	s"spark-submit $fullname" !;
}

run in Compile := spark.value

lazy val hpgator = taskKey[Unit]("copy files to hpgator")
hpgator := { "./src/main/sh/hpgator.sh" !; }
hpgator <<= hpgator.dependsOn(spark)
