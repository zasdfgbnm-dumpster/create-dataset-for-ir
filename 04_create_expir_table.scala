import scala.math._
import org.apache.spark.sql._
import org.apache.spark._
import scala.collection.mutable
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import sys.process._
import scala.language.postfixOps

object CreateExpIRTable {

    class BadJDXException(info:String) extends Throwable {}

    val inputdir = "/home/gaoxiang/create-dataset-for-ir/outputs/01/digitalized/"

    def parse_state(state:String):(String,String) = {
        val pattern = raw"(\w*)(.*)".r
        val pattern(s,i) = state
        val ss = s.toLowerCase.trim
        if(Array("gas","solid","liquid","solution").contains(ss))
            (ss,i.trim)
        else
            ("others",state)
    }

    // convert x,y data to points at standard x
    // Note: xyxy must be ordered ascendingly
    // How to let PriorityQueue order ascending?
    //   val pq = PriorityQueue.empty[(Int, String)](implicitly[Ordering[(Int, String)]].reverse)
    def standard(xyxy:PriorityQueue[(Float,Float)]):Array[Float] = {
        val xmin = 670 //included
        val xmax = 3702 //included
        val xstep = 4
        val vecsize = (xmax-xmin)/xstep + 1
        val yy = new Array[Float](vecsize)

        var x = xmin
        var low_resolution_count = 0
        var (xl,yl) = xyxy.dequeue()
        var idx = 0
        while(x <= xmax){
            val (xr,yr) = xyxy.head
            if(x < xl)
                throw new BadJDXException("out of range")
            else if(x <= xr) {
                yy(idx) = if(x==xl) yl else yl+(yr-yl)/(xr-xl)*(x-xl)
                x += xstep
                idx += 1
                low_resolution_count += 1
            } else {
                val xlyl = xyxy.dequeue()
                xl = xlyl._1
                yl = xlyl._2
                if(low_resolution_count > 0)
                    low_resolution_count -= 1
    		}
    	}

        if(low_resolution_count > 50)
            throw new BadJDXException("resolution too low")
        yy
    }

    def jdx2vec(filename:String):(Array[Float],String,String) = {
        // read records and xyy data
        val source = Source.fromFile(filename)
        val f1 = source.getLines
        val record = mutable.Map[String,String]()
        val xyy = ArrayBuffer[(Float,Array[Float])]()
        var inxyy = false
        record("STATE") = ""
        for( j <- f1 ) {
            if( j.startsWith("##") ) {
                val jsplit = j.split("=",2)
                val varname  = jsplit(0).substring(2).trim()
                val varvalue = jsplit(1).trim()
                (varname,varvalue) match {
                    case ("XYDATA", "(X++(Y..Y))") => inxyy = true
                    case ("END",_) => inxyy = false
                    case _ => record(varname) = varvalue
                }
            } else if(inxyy) {
                // two numbers are not always splited by spaces, e.g.
                // In some files there are strings like "12345-67890",
                // which is 12345 and -67890
                val jfix = j.replaceAll("-"," -")
                val numbers = jfix.split(raw"\s+").map(_.toFloat)
                val x = numbers(0)
                val y = numbers.splitAt(1)._2
                xyy += ( (x,y) )
            }
        }
        source.close()
        // further parse state
        val (state1,state2) = parse_state(record("STATE"))
        // generate [(x,y)..] data, x in 1/CM, y in transmittance
        val xyxy = PriorityQueue.empty[(Float, Float)](implicitly[Ordering[(Float, Float)]].reverse)
        val firstx = record("FIRSTX").toFloat
        val lastx = record("LASTX").toFloat
        val npoints = record("NPOINTS").toFloat
        val deltax = (lastx-firstx)/(npoints-1)
        val yfactor = record("YFACTOR").toFloat
        val xunit = record("XUNITS")
        val yunit = record("YUNITS")
        for( (x,yy) <- xyy ) {
            for( j <- Range(0,yy.length) ){
                var _x = x + deltax*j
                if( xunit == "MICROMETERS" )
                    _x = 10000/_x
                var _y = yy(j)*yfactor
                if( yunit == "ABSORBANCE" )
                    _y = pow(10,-_y).toFloat
                xyxy.enqueue((_x,_y))
            }
        }
        (standard(xyxy),state1,state2)
    }

    def readExpIRAndState(filename:String):ExpIRAndState = {
        // get mid and index
        val ii = filename.split(raw"\.")(0).split("-")
        val mid = ii(0)
        val index = ii(1).toInt
        try {
            val (vec,state,state_info) = jdx2vec(inputdir+filename)
            new ExpIRAndState(mid=mid,index=index,vec=vec,state=state,state_info=state_info)
        } catch {
            case _ : NoSuchElementException  => new ExpIRAndState(mid="",index=0,vec=Array[Float](),state="",state_info="")
            case _ : BadJDXException => new ExpIRAndState(mid="",index=0,vec=Array[Float](),state="",state_info="")
        }
    }

    def main(args: Array[String]): Unit = {
        val session = SparkSession.builder.appName("04_create_expir_table").getOrCreate()
        import session.implicits._

        // process jdx files
        val filelist = session.createDataset( ("ls "+inputdir !!).split(raw"\n") )
        val expir_raw = filelist.map(readExpIRAndState)

        // remove data of bad structures(structures not in mid_structure)
        val mid_structure = session.read.parquet("outputs/03/mid_structure").as[MIDStruct]
        val join = expir_raw.joinWith(mid_structure,expir_raw("mid")===mid_structure("mid"))
        val expir = join.map(_._1)

        // output
        expir.show()
        expir.write.parquet("outputs/04/expir")
        val show_states = expir.groupBy(expir("state")).count().sort($"count".desc)
        show_states.show()
        println("done")
    }

}
