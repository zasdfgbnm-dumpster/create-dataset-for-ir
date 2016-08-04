package irms {

    // info of x
    object X {
        val xmin = 670 //included
        val xmax = 3702 //included
        val xstep = 4
        val vecsize = (xmax-xmin)/xstep + 1
        val xs = Range(X.xmin,X.xmax+X.xstep,X.xstep).map(1.0f*_) // x values
    }
    
    // structure of tables
    case class MIDStruct(mid:String,smiles:String)
    case class ExpIRAndState(mid:String, index:Int, vec:Array[Float], state:String, state_info:String)
    case class StructureUniverse(smiles:String,mass:Float)
    case class TheoreticalIR(smiles:String, method:String, freqs:Array[(Float,Float)])
    
}
