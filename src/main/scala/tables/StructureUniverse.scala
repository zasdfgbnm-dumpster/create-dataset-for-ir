import org.apache.spark.sql._
import sys.process._
import scala.language.postfixOps

package irms {


	@fgargs case class FunctionalGroups()
	object FunctionalGroupsCvt {
		@fgexpand
		def apply(a:Array[Boolean]):FunctionalGroups = FunctionalGroups
	}
	case class StructureUniverse(smiles:String,mass:Double, fg:FunctionalGroups, source:Array[String])
	object StructureUniverse extends Table {

		override val is_large = true

		private val universe_dir = Env.large_raw + "/universe"

		private def parse(pyoutput:String,source:String):StructureUniverse = {
			val (l,fgs) = pyoutput.split(raw"\s+",2+FunGrps.func_grps.length).splitAt(2)
			val fgsboolean = fgs.map(_.toBoolean)
			new StructureUniverse(smiles=l(0),mass=l(1).toDouble,fg=FunctionalGroupsCvt(fgsboolean),source=Array(source))
		}

		def create(path:String):Unit = {
			val spark = Env.spark
			import spark.implicits._

			def smiles_to_table(smiles:Dataset[String],source:String):Dataset[StructureUniverse] = {
				// apply transformations to smiles to generate parquet
				val smstr = smiles.rdd.pipe(Env.pycmd + " " + Env.bin+"/calc-mass-fg.py").toDS()
				smstr.map(parse(_,source))
			}
			// create table for smiles from NIST
			val mid_structure = TableManager.getOrCreate(MIDStruct).as[MIDStruct]
			val nist = smiles_to_table(mid_structure.map(_.smiles).distinct(),"nist")

			// create table for smiles from external source
			def fn_to_table(fn:String):Dataset[StructureUniverse] = {
				val complete_path = universe_dir + "/" + fn
				val smiles = Env.spark.read.text(complete_path).as[String].distinct()
				smiles_to_table(smiles,fn)
			}
			val filenames = ("ls "+universe_dir !!).trim
			val universe_dup = if(filenames.length>0) {
				val external = filenames.split(raw"\n").map(fn_to_table).reduce(_.union(_))
				nist.union(external)
			} else nist

			// merge array of sources by smiles
			def merge_table_elem(a:StructureUniverse,b:StructureUniverse):StructureUniverse = {
				StructureUniverse(smiles=a.smiles,mass=a.mass,fg=a.fg,source=(a.source++b.source).sorted)
			}
			val universe = universe_dup.repartition(2000).map(j=>(j.smiles,j)).rdd.reduceByKey(merge_table_elem).map(_._2).toDS

			universe.show()
			universe.groupBy("source").count().show()
			universe.write.parquet(path)
		}

	}
}
