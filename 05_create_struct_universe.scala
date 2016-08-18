import org.apache.spark.sql._
import org.apache.spark._
import sys.process._
import scala.language.postfixOps

package irms {
    object CreateStructUniverse {

        private val path = "/ufrc/roitberg/qasdfgtyuiop/05_create_struct_universe"
        private val gdb = "/ufrc/roitberg/qasdfgtyuiop/gdb13"

        private def parse(str:String):StructureUniverse = {
            val l = str.split(raw"\s+",2)
            StructureUniverse(smiles=l(0),mass=l(1).toDouble)
        }

        def main(args: Array[String]): Unit = {
            val session = SparkSession.builder.appName("05_create_struct_universe").getOrCreate()
            import session.implicits._

            // get list of smiles
            val mid_structure = session.read.parquet("outputs/tables/mid_structure").as[MIDStruct]
            val smiles_nist = mid_structure.map(_.smiles).distinct()
            val filenames = Range(1,14).map(j=>gdb+s"/$j.smi")
            val smiles_gdb = filenames.map(session.read.text(_).as[String]).reduce(_.union(_))
            val smiles = smiles_nist.union(smiles_gdb).repartition(2000).distinct()
            print("number of structures: "); println(smiles.count())

            // apply transformations to smiles to generate parquet
            val smstr = smiles.rdd.pipe(path+"/tools/calc-mass.py").toDS()
            val universe = smstr.map(parse)

            universe.show()
            universe.write.parquet("outputs/tables/universe")
        }

    }
}
