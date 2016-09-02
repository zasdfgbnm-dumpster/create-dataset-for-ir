package irms {
	object Main {
		def main(args: Array[String]): Unit = {
			MIDStruct.getOrCreate
			TheoreticalIR.getOrCreate
			ExpIRAndState.getOrCreate
			StructureUniverse.getOrCreate
		}

	}
}
