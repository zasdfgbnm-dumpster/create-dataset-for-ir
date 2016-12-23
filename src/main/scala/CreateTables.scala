package irms {
	object CreateTables {
		def main(args: Array[String]): Unit = {
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

	}
}
