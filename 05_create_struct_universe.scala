import org.apache.spark.sql._
import org.apache.spark._
import sys.process._

package irms {
    object CreateStructUniverse {

        private val gdb = Environment.raw + "/gdb13"

        private def parse(str:String):StructureUniverse = {
            val l = str.split(raw"\s+",2+FunGrps.func_grps.length)
            StructureUniverse(smiles=l(0),mass=l(1).toDouble)
        }

        def main(args: Array[String]): Unit = {
            val session = SparkSession.builder.appName("05_create_struct_universe").getOrCreate()
            import session.implicits._

            // get list of smiles
            val mid_structure = session.read.parquet(Environment.tables+"/mid_structure").as[MIDStruct]
            val smiles_nist = mid_structure.map(_.smiles).distinct()
            val smiles =
                if(Environment.ishpg){
                    val filenames = Range(1,14).map(j=>gdb+s"/$j.smi")
                    val smiles_gdb = filenames.map(session.read.text(_).as[String]).reduce(_.union(_))
                    smiles_nist.union(smiles_gdb).repartition(2000).distinct()
                }else
                    smiles_nist

            println("functional groups: "+FunGrps.func_grps.reduce(_+" "+_))
            println("number of structures: "+smiles.count())

            // apply transformations to smiles to generate parquet
            val smstr = smiles.rdd.pipe(Environment.pycmd + " " + Environment.bin+"/calc-mass-fg.py").toDS()
            val universe = smstr.map(parse)

            universe.show()
            //universe.write.parquet("outputs/tables/universe")
        }

    }
}
