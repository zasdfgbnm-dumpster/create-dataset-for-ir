package irms {

    // info of x
    object X {
        val xmin = 670 //included
        val xmax = 3702 //included
        val xstep = 4
        val vecsize = (xmax-xmin)/xstep + 1
        val xs = Range(X.xmin,X.xmax+X.xstep,X.xstep).map(1d*_) // x values
    }

    // structure of tables
    case class MIDStruct(mid:String,smiles:String)
    case class ExpIRAndState(mid:String, index:Int, vec:Array[Double], state:String, state_info:String)
    case class StructureUniverse(smiles:String,mass:Double)
    case class TheoreticalIR(smiles:String, method:String, freqs:Array[(Double,Double)])

    // environment
    object Environment {
        import java.nio.file.{Paths, Files}
        val ishpg = Files.exists(Paths.get("/ufrc/roitberg/qasdfgtyuiop"))
        val workspace = if(ishpg) "/ufrc/roitberg/qasdfgtyuiop/" else "/home/gaoxiang/MEGA"
        val raw = workspace + "/raw"
        val tables = workspace + "/tables"
        val bin = workspace + "/bin"
        val tmp = workspace + "/tmp"
        val data = workspace + "/data"
        val pycmd = if(ishpg) bin+"/anaconda3/envs/my-rdkit-env/bin/python" else "python"
    }
    //TODO: change all current codes to use this

    object FunGrps {
        import scala.io.Source
        val func_grps:Seq[String] = {
            val fn = Environment.data + "/FunctionalGroups.txt"
            val lines = Source.fromFile(fn).getLines.toList
            val uncomment = lines.map(_.mkString.split("//")(0).trim)
            val fields = uncomment.map(_.split("\t"))
            val selected = fields.filter(_.length==2)
            selected.map(_(1).trim)
        }
    }

}
