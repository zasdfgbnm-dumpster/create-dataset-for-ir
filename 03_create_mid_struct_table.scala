import org.apache.spark.sql._
import org.apache.spark._
import sys.process._
import scala.language.postfixOps

object CreateMIDStructTable {
    case class DupOf(mid:String,dupof:String)
    def main(args: Array[String]):Unit = {
        val session = SparkSession.builder.appName("03_create_mid_struct_table").getOrCreate()
        import session.implicits._

        // process structures
        val path = "/home/gaoxiang/create-dataset-for-ir/outputs/02"
        val lsalts  = session.read.text(path + "/all.salts.smi").as[String]
        val lunique = session.read.text(path + "/all.unique.smi").as[String]
        val lmix    = session.read.text(path + "/all.mixtures.smi").as[String]

        def line2row(line:String):MIDStruct = {
            val l = line.split(raw"\s+")
            MIDStruct(mid=l(1),smiles=l(0))
        }

        val structs = lsalts.union(lunique).union(lmix).map(line2row)

        // process duplicates
        val ldup = session.read.text(path + "/duplicates.log").as[String]

        def dup_line2row(line:String):DupOf = {
            val l = line.split(raw"\s+")
            DupOf(mid=l(1),dupof=l(6))
        }

        val dup = ldup.map(dup_line2row)

        // merge duplicates and normal
        val join = dup.joinWith(structs,dup("dupof")===structs("mid"))
        val dup_struct = join.map( j => new MIDStruct(j._1.mid,j._2.smiles) )
        val total_struct = dup_struct.union(structs)

        // verify structures
        def verify_struc(smiles:String):Boolean = ( ( s"./tools/verify.py $smiles" ! ) == 0 )
        val filtered = total_struct.filter( r => verify_struc(r.smiles) )

        // write to file
        filtered.write.parquet("outputs/03/mid_structure")
        filtered.show()
        println("done")
    }
}
