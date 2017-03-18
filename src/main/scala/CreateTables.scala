import org.mongodb.scala._
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration

package irms {
	object CreateTables {

		def wait_results[C](observable:Observable[C]): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))

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

		def main(args: Array[String]): Unit = {

			// show all tables
			val spark = Env.spark
			import spark.implicits._
			val du = TableManager.getOrCreate(DatasetUsage).as[DatasetUsage]
			val midstruct =  TableManager.getOrCreate(MIDStruct).as[MIDStruct]
			val thir = TableManager.getOrCreate(TheoreticalIR).as[TheoreticalIR]
			val expir =  TableManager.getOrCreate(ExpIRAndState).as[ExpIRAndState]
			val univ = TableManager.getOrCreate(StructureUniverse).as[StructureUniverse]
			du.show()
			midstruct.show()
			thir.show()
			expir.show()
			univ.show()

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


			// export to MongoDB
			val mongoClient: MongoClient = MongoClient()
			val database: MongoDatabase = mongoClient.getDatabase("test")
			val collection: MongoCollection[Document] = database.getCollection("test")
			val doc: Document = Document("name" -> "MongoDB", "type" -> "database","count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))
			wait_results(collection.insertOne(doc))
			println("done")
		}

	}
}
