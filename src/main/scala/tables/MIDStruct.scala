import org.apache.spark.sql._

package irms {

    import Env.spark.implicits._

    case class MIDStruct(mid:String,smiles:String)
    object MIDStruct extends ProductTable[MIDStruct] {

        private case class DupOf(mid:String,dupof:String)

        def create(path:String):Unit = {
            import Env.spark.implicits._

            // process structures
            val lunique = Env.spark.read.text(Env.raw + "/all.unique.smi").as[String]
            //val lsalts  = Env.spark.read.text(Env.raw + "/all.salts.smi").as[String]
            //val lmix    = Env.spark.read.text(Env.raw + "/all.mixtures.smi").as[String]

            def line2row(line:String):MIDStruct = {
                val l = line.split(raw"\s+")
                MIDStruct(mid=l(1),smiles=l(0))
            }
            val structs_not_validated = lunique/*.union(lsalts).union(lmix)*/.map(line2row)

            // validate structures
            val smiles = structs_not_validated.repartition(32).map(j=>j.smiles).rdd
                         .pipe(Env.pycmd + " " + Env.bin + "/verify.py").toDS
            val structs = structs_not_validated.joinWith(smiles,structs_not_validated("smiles")===smiles("value")).map(_._1)
            //println("number of bad structures removed: "+(structs_not_validated.count()-structs.count()))

            // process duplicates
            val ldup = Env.spark.read.text(Env.raw + "/duplicates.log").as[String]

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
            total_struct.show()
            total_struct.write.parquet(path)
        }
    }
}
