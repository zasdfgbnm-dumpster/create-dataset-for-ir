import org.apache.spark.sql._

package irms {

    import Env.spark.implicits._

    @fgargs case class FunctionalGroups()
    case class StructureUniverse(smiles:String,mass:Double, fg:FunctionalGroups)
    object StructureUniverse extends Table[StructureUniverse] {

        private val gdb = Env.raw + "/gdb13"

        private def parse(str:String):StructureUniverse = {
            val l = str.split(raw"\s+",2+FunGrps.func_grps.length)
            StructureUniverse(smiles=l(0),mass=l(1).toDouble)
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
            println("number of structures: "+smiles.count())

            // apply transformations to smiles to generate parquet
            val smstr = smiles.rdd.pipe(Env.pycmd + " " + Env.bin+"/calc-mass-fg.py").toDS()
            val universe = smstr.map(parse)

            universe.show()
            universe.write.parquet(path)
        }

    }
}
