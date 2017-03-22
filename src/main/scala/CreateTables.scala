import org.mongodb.scala._
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration

package irms {
	object CreateTables {

		def wait_results[C](observable:Observable[C]): Seq[C] = Await.result(observable.toFuture(), Duration.Inf)

		def create_tables(args: Array[String]): Unit = {
			val arglist = (if(args.isEmpty) Array("DatasetUsage","MIDStruct","TheoreticalIR","ExpIRAndState","StructureUniverse") else args).toList

			val spark = Env.spark
			import spark.implicits._

			for(i<-arglist){
				i match {
					case "DatasetUsage" => TableManager.getOrCreate(DatasetUsage)
					case "MIDStruct" => TableManager.getOrCreate(MIDStruct)
					case "TheoreticalIR" => TableManager.getOrCreate(TheoreticalIR)
					case "ExpIRAndState" => TableManager.getOrCreate(ExpIRAndState)
					case "StructureUniverse" => TableManager.getOrCreate(StructureUniverse)
				}
			}
		}

		case class SmExpir(smiles:String,vec:Array[Double],state:String,state_info:String)

		def create_documents(args: Array[String]): Unit = {

			// show all tables
			val spark = Env.spark
			import spark.implicits._

			val du = TableManager.getOrCreate(DatasetUsage).distinct()//.as[DatasetUsage]
			val midstruct =  TableManager.getOrCreate(MIDStruct).distinct()//.as[MIDStruct]
			val thir = TableManager.getOrCreate(TheoreticalIR).distinct()//.as[TheoreticalIR]
			val expir =  TableManager.getOrCreate(ExpIRAndState).distinct()//.as[ExpIRAndState]
			val univ = TableManager.getOrCreate(StructureUniverse).repartition(2000).distinct()//.as[StructureUniverse]
			// du.show()
			// midstruct.show()
			// thir.show()
			// expir.show()
			// univ.show()

			// convert to document of this form:
			// 	{
			// 		_id: "smiles"
			// 		mass: 1.234
			// 		fg: {
			// 			ketone: false
			// 		}
			// 		from: [ "nist", "gdb3" ]
			// 		external_id : {
			// 			nist: "C3131312"
			// 		}
			// 		expir: [{
			// 			state:"gas"
			//			state_info: "10% KCl..."
			//			vec: [ 0.1, 0.2, 0.3 ]
			// 		}]
			// 		thir: [
			// 			{
			// 				method: "B3LYP/6-31G*"
			// 				freqs: [ {
			// 					freq: 1234
			// 					intensity: 2233423
			// 				} ]
			// 			}
			// 		]
			// 	}

			// convert expir to an rdd of Document arrays as shown above
			val smexpir = expir.join(midstruct, expir("mid")===midstruct("mid")).select("smiles","vec","state","state_info").distinct().as[SmExpir]
			def expir2doc(r:SmExpir):(String,Document) = (r.smiles,Document("expir"->List(Document("vec"->r.vec.toList,"state"->r.state,"state_info"->r.state_info))))
			val smexpirkeydoc = smexpir.rdd.map(expir2doc).reduceByKey(_++_).distinct()

			// convert thir to an rdd of Document arrays as shown above
			def thir2doc(r:TheoreticalIR):(String,Document) = {
				val freqs = r.freqs.map(j=>Document("freq"->j._1,"intensity"->j._2)).toList
				(r.smiles,Document("thir"->List(Document("method"->r.method,"freqs"->freqs))))
			}
			val thirkeydoc = thir.as[TheoreticalIR].rdd.map(thir2doc).reduceByKey(_++_).distinct()

			// convert mid struct map to an rdd of Document arrays as shown above
			def midlist2doc(a:List[String]):Document = Document("external_id"->Document("nist"->a))
			val midstructkeydoc = midstruct.as[MIDStruct].rdd.map(r=>(r.smiles,List(r.mid))).reduceByKey(_++_).mapValues(midlist2doc).distinct()

			// convert universe to an rdd of Document arrays as shown above
			def universe2doc(r:StructureUniverse):(String,Document) = {
				def getval(f:java.lang.reflect.Field) = {
					f.setAccessible(true)
					f.get(r.fg)
				}
				val fg = r.fg.getClass.getDeclaredFields.map(j=>Document(j.getName->getval(j).asInstanceOf[Boolean])).reduce(_++_)
				(r.smiles,Document("mass"->r.mass,"source"->r.source.toList,"fg"->fg))
			}
			val univkeydoc = univ.as[StructureUniverse].rdd.map(universe2doc _)

			// merge thir, expir, external_id
			val merged = smexpirkeydoc.union(thirkeydoc).union(midstructkeydoc).reduceByKey(_++_).distinct().union(univkeydoc).repartition(2000).reduceByKey(_++_,2000).distinct(2000)
			merged.map(j=>Document("_id"->j._1)++j._2).saveAsObjectFile("/mnt/data/gaoxiang/spark-mongodb/universe")
			// export to MongoDB
			// def add_to_db(docs:Iterator[Document]) = {
			// 	val docseq = docs.toSeq
			// 	val mongoClient: MongoClient = MongoClient()
			// 	val database: MongoDatabase = mongoClient.getDatabase("irms")
			// 	val collection: MongoCollection[Document] = database.getCollection("universe")
			// 	println("begin inserting " + docseq.length + "data into db, wait patiently...")
			// 	wait_results(collection.insertMany(docseq))
			// 	println("done inserting " + docseq.length + "data into db.")
			// }
			// merged.map(j=>Document("_id"->j._1)++j._2).foreachPartition(add_to_db)
		}

		def main(args: Array[String]): Unit = {
		}

	}
}
