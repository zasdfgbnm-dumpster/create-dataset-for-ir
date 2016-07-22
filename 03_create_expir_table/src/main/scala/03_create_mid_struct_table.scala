import org.apache.spark.sql._
import org.apache.spark._

object CreateMIDStructTable {
    def main(args: Array[String]) {
        val session = SparkSession.builder.appName("03_create_mid_struct_table").getOrCreate()
        import session.implicits._

        // process structures
        val path = "/home/gaoxiang/create-dataset-for-ir/outputs/02"
        val lsalts  = session.read.text(path + "/all.salts.smi").as[String]
        val lunique = session.read.text(path + "/all.unique.smi").as[String]
        val lmix    = session.read.text(path + "/all.mixtures.smi").as[String]

        case class MIDStruct(mid:String,structure:String)

        def line2row(line:String):MIDStruct = {
            val l = line.split(raw"\s+")
            MIDStruct(mid=l(1),structure=l(0))
        }

        val structs = lsalts.union(lunique).union(lmix).map(line2row)

        // process duplicates
        val ldup = session.read.text(path + "/duplicates.log").as[String]

        case class DupOf(mid:String,dupof:String)

        def dup_line2row(line:String):DupOf = {
            val l = line.split(raw"\s+")
            DupOf(mid=l(1),dupof=l(6))
        }

        val dup = ldup.map(dup_line2row)

        // merge duplicates and normal
        val dup_struct = dup.joinWith(structs,dup("dupof")===structs("mid")).select(dup("mid"),structs("structure")).as[MIDStruct]
        val total_struct = structs.union(dup_struct)

        // write to file
        total_struct.write.parquet("outputs/03/mid_structure")
        total_struct.show()
        println("done")
    }
}
