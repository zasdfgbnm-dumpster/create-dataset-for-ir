import org.apache.spark.sql._
import org.apache.spark._

package irms {
    object CreateMIDStructTable {

        private case class DupOf(mid:String,dupof:String)

        def main(args: Array[String]):Unit = {
            val session = SparkSession.builder.appName("03_create_mid_struct_table").getOrCreate()
            import session.implicits._

            // process structures
            val path = "outputs/02"
            val lunique = session.read.text(path + "/all.unique.smi").as[String]
            //val lsalts  = session.read.text(path + "/all.salts.smi").as[String]
            //val lmix    = session.read.text(path + "/all.mixtures.smi").as[String]

            def line2row(line:String):MIDStruct = {
                val l = line.split(raw"\s+")
                MIDStruct(mid=l(1),smiles=l(0))
            }
            val structs_not_validated = lunique/*.union(lsalts).union(lmix)*/.map(line2row)

            // validate structures
            val smiles = structs_not_validated.repartition(32).map(j=>j.smiles).rdd.pipe("tools/verify.py").toDS
            val structs = structs_not_validated.joinWith(smiles,structs_not_validated("smiles")===smiles("value")).map(_._1)
            println("number of structures not validated: ",structs_not_validated.count())
            println("number of structures validate: ",structs.count())

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

            // write to file
            total_struct.write.parquet("outputs/tables/mid_structure")
            total_struct.show()
            println("done")
        }
    }
}
