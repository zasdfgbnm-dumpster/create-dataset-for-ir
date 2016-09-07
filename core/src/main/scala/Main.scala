package irms {
	object Main {
		def main(args: Array[String]): Unit = {
			val arglist = (if(args.isEmpty) Array(MIDStruct.getTableName,TheoreticalIR.getTableName,ExpIRAndState.getTableName,StructureUniverse.getTableName) else args).toList
			for(i<-arglist){
				i match {
					case MIDStruct.getTableName => { MIDStruct.getOrCreate; MIDStruct.stats }
					case TheoreticalIR.getTableName => { TheoreticalIR.getOrCreate; TheoreticalIR.stats }
					case ExpIRAndState.getTableName => { ExpIRAndState.getOrCreate; ExpIRAndState.stats }
					case StructureUniverse.getTableName => { StructureUniverse.getOrCreate; StructureUniverse.stats }
				}
			}
		}

	}
}
