//import sbt.NameFilter
import sbt.complete._
//import sbt.complete.Parser._
import sbt.complete.DefaultParsers._
//import sbt.complete.DefaultParsers._

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
val parser:Parser[Seq[String]] = token( (Space~>literal("ExpIRAndState")) | (Space~>literal("MIDStruct")) | (Space~>literal("StructureUniverse")) | (Space~>literal("TheoreticalIR")) ).*

val rm = inputKey[Unit]("remove a table")
rm := {
	val args = parser.parsed
	println("rm: "+args)
	if(args.isEmpty)
		"find "+tables+" -maxdepth 1 -mindepth 1 -exec rm -rf {} ;" !;
	else
		s"rm -rf "+args.map(s"$tables/"+_).mkString(" ") !;
}

run in Compile := {
	val args = parser.parsed
	val jar = (Keys.`package` in Compile).value
	val fullname = jar.getAbsolutePath()
	"./src/main/sh/create-links.sh" !;
	s"spark-submit $fullname "+args.mkString(" ") !;
}

// val regen = inputKey[Unit]("remove old table and regenerate new table")
// regen := {
// 	val arg:String = parser.parsed.mkString(" ")
// 	rm.evaluated
// 	(run in Compile).evaluated
// }

// lazy val hpgator = taskKey[Unit]("copy files to hpgator")
// hpgator := { "./src/main/sh/hpgator.sh" !; }
// hpgator <<= hpgator.dependsOn(run)
