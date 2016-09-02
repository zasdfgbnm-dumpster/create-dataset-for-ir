package irms {
	object Main {
		def main(args: Array[String]): Unit = {
			val arglist = (if(args.isEmpty) Array(MIDStruct.getTableName,TheoreticalIR.getTableName,ExpIRAndState.getTableName,StructureUniverse.getTableName) else args).toList
			for(i<-arglist){
				i match {
					case MIDStruct.getTableName => MIDStruct.getOrCreate
					case TheoreticalIR.getTableName => TheoreticalIR.getOrCreate
					case ExpIRAndState.getTableName => ExpIRAndState.getOrCreate
					case StructureUniverse.getTableName => StructureUniverse.getOrCreate
				}
			}
		}

	}
}
