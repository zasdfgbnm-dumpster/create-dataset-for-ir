import org.apache.spark.sql._

package irms {

    import Env.spark.implicits._

    @fgargs case class FunctionalGroups()
    object FunctionalGroupsCvt {
        @fgexpand
        def apply(a:Array[Boolean]):FunctionalGroups = FunctionalGroups
    }
    case class StructureUniverse(smiles:String,mass:Double, fg:FunctionalGroups)
    object StructureUniverse extends Table[StructureUniverse] {

        private val gdb = Env.raw + "/gdb13"

        private def parse(str:String):StructureUniverse = {
            val (l,fgs) = str.split(raw"\s+",2+FunGrps.func_grps.length).splitAt(2)
            val fgsboolean = fgs.map(_.toBoolean)
            StructureUniverse(smiles=l(0),mass=l(1).toDouble,fg=FunctionalGroupsCvt(fgsboolean))
        }

        def create(path:String):Unit = {
            import Env.spark.implicits._

            // get list of smiles
            val mid_structure = MIDStruct.getOrCreate
            val smiles_nist = mid_structure.map(_.smiles).distinct()
            val smiles =
                if(Env.ishpg){
                    val filenames = Range(1,14).map(j=>gdb+s"/$j.smi")
                    val smiles_gdb = filenames.map(Env.spark.read.text(_).as[String]).reduce(_.union(_))
                    smiles_nist.union(smiles_gdb).repartition(2000).distinct()
                }else
                    smiles_nist

            println("functional groups: "+FunGrps.func_grps.reduce(_+" "+_))

            // apply transformations to smiles to generate parquet
            val smstr = smiles.rdd.pipe(Env.pycmd + " " + Env.bin+"/calc-mass-fg.py").toDS()
            val universe = smstr.map(parse)

            universe.write.parquet(path)
        }

        def stats() = {
            val universe = getOrCreate
            val masses = universe.groupBy("mass").count().sort($"count".desc)
            val massfgs = universe.groupBy("mass","fg").count().sort($"count".desc)
            universe.show()
            masses.show()
            massfgs.show()
            massfgs.write.parquet("demos/massfgs")
        }

    }
}
