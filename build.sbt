import sbt.complete._
import sbt.complete.DefaultParsers._
import java.io.File

val scalaversion = "2.11.8"
val paradiseVersion = "2.1.0"

val commonSettings = Defaults.defaultSettings ++ Seq (
	scalaVersion := scalaversion,
	scalacOptions ++= Seq("-Xlint","-feature","-deprecation"),
	crossScalaVersions := Seq(scalaversion),
	resolvers += Resolver.sonatypeRepo("snapshots"),
	resolvers += Resolver.sonatypeRepo("releases"),
	addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
)

// projects
lazy val macro = (project in file("macro")).
	settings(commonSettings: _*).
	settings (
		libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),
		libraryDependencies ++= ( if(scalaVersion.value.startsWith("2.10")) List("org.scalamacros" %% "quasiquotes" % paradiseVersion) else Nil )
	)

lazy val root = (project in file(".")).dependsOn(macro).settings(commonSettings: _*)


// project root
name := "CreateDataset"
version := "0.0.1-SNAPSHOT"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"
cleanFiles <+= baseDirectory { base => base / "spark-warehouse" }
mainClass in assembly := Some("irms.CreateTables")
assemblyMergeStrategy in assembly := {
	case "FunctionalGroups.txt" => MergeStrategy.deduplicate
	case PathList("irms", xs @ _*) => MergeStrategy.deduplicate
	case x => MergeStrategy.discard
}

//paths
//TODO: dedup path below
val workspace = System.getProperty("user.home")+"/MEGA"
val bin = workspace + "/bin"
val tables = workspace + "/tables"

//tasks
run in Compile := {
	val args = parser.parsed
	val jar = (assembly in assembly).value
	val fullname = jar.getAbsolutePath()
	"./src/main/sh/create-links.sh" !;
	s"spark-submit $fullname "+args.mkString(" ") !;
}

val parser:Parser[Seq[String]] = token( (Space~>literal("ExpIRAndState")) | (Space~>literal("MIDStruct")) | (Space~>literal("StructureUniverse")) | (Space~>literal("TheoreticalIR")) ).*

lazy val rm = inputKey[Unit]("remove a table")
rm := {
	val args = parser.parsed
	if(args.isEmpty)
		"find "+tables+" -maxdepth 1 -mindepth 1 -exec rm -rf {} ;" !;
	else
		s"rm -rf "+args.map(s"$tables/"+_).mkString(" ") !;
}

// lazy val regen = inputKey[Unit]("remove old table and regenerate new table")
// regen := {
// 	val arg:String = parser.parsed.mkString(" ")
// 	rm.evaluated
// 	(run in Compile).evaluated
// }

// copy files to hipergator
lazy val hpgator = taskKey[Unit]("copy files to hpgator")
hpgator := {
	val jar = (assembly in assembly).value.getAbsolutePath()
	val build_tables = (run in Compile).toTask(" MIDStruct ExpIRAndState TheoreticalIR").value
	"./src/main/sh/hpgator.sh " + jar !;
}

// convert tables to json
lazy val json = taskKey[Unit]("convert tables to json")
json := {
	val build_tables = (assembly in assembly).value
	"python" #< new File("tools/json.py") !
}
