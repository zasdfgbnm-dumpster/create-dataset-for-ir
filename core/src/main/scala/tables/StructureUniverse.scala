import org.apache.spark.sql._
import sys.process._
import scala.language.postfixOps

package irms {

    import Env.spark.implicits._

    @fgargs case class FunctionalGroups()
    object FunctionalGroupsCvt {
        @fgexpand
        def apply(a:Array[Boolean]):FunctionalGroups = FunctionalGroups
    }
    case class StructureUniverse(smiles:String,mass:Double, fg:FunctionalGroups)
    object StructureUniverse extends ProductTable[StructureUniverse] {

        private val universe_dir = Env.raw + "/universe"

        private def parse(str:String):StructureUniverse = {
            val (l,fgs) = str.split(raw"\s+",2+FunGrps.func_grps.length).splitAt(2)
            val fgsboolean = fgs.map(_.toBoolean)
            StructureUniverse(smiles=l(0),mass=l(1).toDouble,fg=FunctionalGroupsCvt(fgsboolean))
        }

        def create(path:String):Unit = {
            import Env.spark.implicits._

            // get list of smiles
            val mid_structure = TableManager.getOrCreate(MIDStruct)
            val smiles_nist = mid_structure.map(_.smiles).distinct()
            val filenames = ("ls "+universe_dir !!).trim
            val smiles = if(filenames.length>0) {
                val fn = filenames.split(raw"\n").map(universe_dir+"/"+_)
                val smiles_universe = fn.map(Env.spark.read.text(_).as[String]).reduce(_.union(_))
                smiles_nist.union(smiles_universe).repartition(2000).distinct()
            } else smiles_nist

            // apply transformations to smiles to generate parquet
            val smstr = smiles.rdd.pipe(Env.pycmd + " " + Env.bin+"/calc-mass-fg.py").toDS()
            val universe = smstr.map(parse)

            universe.write.parquet(path)
        }

        def stats() = {
            println("number of functional groups: "+FunGrps.func_grps.reduce(_+" "+_))
            val universe = TableManager.getOrCreate(this)
            val masses = universe.groupBy("mass").count().sort($"count".desc)
            val massfgs = universe.groupBy("mass","fg").count().sort($"count".desc)
            val fgs = universe.groupBy("fg").count().sort($"count".desc)
            universe.show()
            masses.show()
            massfgs.show()
            fgs.show()
            if(Env.ishpg) {
                masses.write.parquet("demos/mass")
                massfgs.write.parquet("demos/massfgs")
                fgs.write.parquet("demos/fgs")
            }
        }

    }
}
