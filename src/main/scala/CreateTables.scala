package irms {
	object CreateTables {
		val spark = Env.spark
		def main(args: Array[String]): Unit = {
			val arglist = (if(args.isEmpty) Array("MIDStruct","TheoreticalIR","ExpIRAndState","StructureUniverse") else args).toList
			import spark.implicits._
			for(i<-arglist){
				i match {
					case "MIDStruct" => TableManager.getOrCreate(MIDStruct)
					case "TheoreticalIR" => TableManager.getOrCreate(TheoreticalIR)
					case "ExpIRAndState" => TableManager.getOrCreate(ExpIRAndState)
					case "StructureUniverse" => TableManager.getOrCreate(StructureUniverse)
				}
			}
		}

	}
}
