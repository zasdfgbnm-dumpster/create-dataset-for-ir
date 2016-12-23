import org.apache.spark.sql._

package irms {

    case class DatasetUsage(mid:String, index:Int, level:String ,usage:String)
    object DatasetUsage extends Table {

        private case class Joined(mid:String,index:Int,state:String,source:Array[String])

        def create(path:String):Unit = {

            val spark = Env.spark
            import spark.implicits._

            val expir = TableManager.getOrCreate(ExpIRAndState).select("mid","index","state")//.as[ExpIRAndState]
            val midstruct = TableManager.getOrCreate(MIDStruct).select("mid","smiles")//.as[MIDStruct]
            val universe = TableManager.getOrCreate(StructureUniverse).select("smiles","source")//.as[StructureUniverse]
            val join = expir.join(midstruct,"mid").join(universe,"smiles").select("mid","index","state","source").as[Joined]

            def assign_level(j:Joined):DatasetUsage = {
                if(j.state=="gas") {
                    if(j.source.length>1)
                        DatasetUsage(j.mid,j.index,"overall","TBD")
                    else
                        DatasetUsage(j.mid,j.index,"limited","TBD")
                } else {
                    DatasetUsage(j.mid,j.index,"unused","unused")
                }
            }
            val wlevels = join.map(assign_level)

            def assign_usage(ds:Dataset[DatasetUsage],weights:Array[Double]):Dataset[DatasetUsage] = {
                val split = ds.randomSplit(weights)
                val train = split(0).map(j=>DatasetUsage(j.mid,j.index,j.level,"training"))
                val valid = split(1).map(j=>DatasetUsage(j.mid,j.index,j.level,"validation"))
                val test  = split(2).map(j=>DatasetUsage(j.mid,j.index,j.level,"testing"))
                train.union(valid).union(test)
            }
            val overall = assign_usage(wlevels.filter(_.level=="overall"),Array(6.0,3.0,3.0))
            val limited = assign_usage(wlevels.filter(_.level=="limited"),Array(5.0,1.0,1.0))
            val unused = wlevels.filter(_.level=="unused")
            val result = overall.union(limited).union(unused)

            result.groupBy("level","usage").count().show()
            result.write.parquet(path)
        }
	}
}
