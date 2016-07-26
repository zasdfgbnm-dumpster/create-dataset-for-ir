case class MIDStruct(mid:String,smiles:String)
case class ExpIRAndState(mid:String, index:Int, vec:Array[Float], state:String, state_info:String)
case class StructureUniverse(smiles:String,mass:Float)
case class TheoreticalIR(smiles:String, method:String, freqs:Array[(Float,Float)])
