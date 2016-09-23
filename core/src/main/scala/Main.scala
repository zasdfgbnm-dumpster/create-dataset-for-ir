package irms {
	object Main {
		def main(args: Array[String]): Unit = {
			val arglist = (if(args.isEmpty) Array(MIDStruct.getTableName,TheoreticalIR.getTableName,ExpIRAndState.getTableName,StructureUniverse.getTableName) else args).toList
			import Env.spark.implicits._
			for(i<-arglist){
				i match {
					case MIDStruct.getTableName => TableManager.getOrCreate(MIDStruct)
					case TheoreticalIR.getTableName => TableManager.getOrCreate(TheoreticalIR)
					case ExpIRAndState.getTableName => TableManager.getOrCreate(ExpIRAndState)
					case StructureUniverse.getTableName => TableManager.getOrCreate(StructureUniverse)
				}
			}
		}

	}
}
