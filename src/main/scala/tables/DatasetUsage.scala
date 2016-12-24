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

            // now we only use gas phase data, therefore data of other state are set to "unused"
            // because GDB13 is far from complete, all we can do is to use all data that happens to be inside GDB13 as level overall
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
                val all = train.union(valid).union(test)
                // data with the same mid should have the same usage
                val r = scala.util.Random
                def rs(a:String,b:String):String = if(r.nextBoolean) a else b
                val dups = all.map(j=>(j.mid,(j.usage,true))).rdd.reduceByKey( (i,j) => ( rs(i._1,j._1), (i._1==j._1) && i._2 && j._2 ) ).filter(!_._2._2).map(j=>(j._1,j._2._1)).toDS
                val m = dups.collect().toMap
                def fix_usage(du:DatasetUsage) = DatasetUsage(du.mid,du.index,du.level,if(m.contains(du.mid)) m(du.mid) else du.usage)
                all.map(fix_usage)
            }
            val overall = assign_usage(wlevels.filter(_.level=="overall"),Array(5.0,3.0,3.0))
            val limited = assign_usage(wlevels.filter(_.level=="limited"),Array(4.0,1.0,1.0))
            val unused = wlevels.filter(_.level=="unused")
            val result = overall.union(limited).union(unused)

            result.groupBy("level","usage").count().show()
            result.write.parquet(path)
        }
	}
}
