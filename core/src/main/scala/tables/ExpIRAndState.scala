import scala.math._
import scala.collection.mutable
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import sys.process._
import scala.language.postfixOps
import org.apache.spark.sql._

package irms {

    import Env.spark.implicits._

    case class ExpIRAndState(mid:String, index:Int, vec:Array[Double], state:String, state_info:String)
    object ExpIRAndState extends Table[ExpIRAndState] {

        private class BadJDXException(info:String) extends Throwable {}

        private val inputdir = Env.raw + "/digitalized/"

        private def parse_state(state:String):(String,String) = {
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
        private def standard(xyxy:PriorityQueue[(Double,Double)]):Array[Double] = {
            val yy = new Array[Double](Params.X.vecsize)

            var x = Params.X.xmin
            var low_resolution_count = 0
            var (xl,yl) = xyxy.dequeue()
            var idx = 0
            while(x <= Params.X.xmax){
                val (xr,yr) = xyxy.head
                if(x < xl)
                    throw new BadJDXException("out of range")
                else if(x <= xr) {
                    yy(idx) = if(x==xl) yl else yl+(yr-yl)/(xr-xl)*(x-xl)
                    x += Params.X.xstep
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

        private def jdx2vec(filename:String):(Array[Double],String,String) = {
            // read records and xyy data
            val source = Source.fromFile(filename)
            val f1 = source.getLines
            val record = mutable.Map[String,String]()
            val xyy = ArrayBuffer[(Double,Array[Double])]()
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
                    val numbers = jfix.split(raw"\s+").map(_.toDouble)
                    val x = numbers(0)
                    val y = numbers.splitAt(1)._2
                    xyy += ( (x,y) )
                }
            }
            source.close()
            // further parse state
            val (state1,state2) = parse_state(record("STATE"))
            // generate [(x,y)..] data, x in 1/CM, y in transmittance
            val xyxy = PriorityQueue.empty[(Double, Double)](implicitly[Ordering[(Double, Double)]].reverse)
            val firstx = record("FIRSTX").toDouble
            val lastx = record("LASTX").toDouble
            val npoints = record("NPOINTS").toDouble
            val deltax = (lastx-firstx)/(npoints-1)
            val yfactor = record("YFACTOR").toDouble
            val xunit = record("XUNITS")
            val yunit = record("YUNITS")
            for( (x,yy) <- xyy ) {
                for( j <- Range(0,yy.length) ){
                    var _x = x + deltax*j
                    if( xunit == "MICROMETERS" )
                        _x = 10000/_x
                    var _y = yy(j)*yfactor
                    if( yunit == "ABSORBANCE" )
                        _y = 1 - pow(10,-_y).toDouble
                    else
                        _y = 1 - _y
                    xyxy.enqueue((_x,_y))
                }
            }
            (standard(xyxy),state1,state2)
        }

        private def readExpIRAndState(filename:String):Option[ExpIRAndState] = {
            // get mid and index
            val ii = filename.split(raw"\.")(0).split("-")
            val mid = ii(0)
            val index = ii(1).toInt
            try {
                val (vec,state,state_info) = jdx2vec(inputdir+filename)
                Some(new ExpIRAndState(mid=mid,index=index,vec=vec,state=state,state_info=state_info))
            } catch {
                case _ : NoSuchElementException  => None
                case _ : BadJDXException => None
            }
        }

        def create(path:String):Unit = {
            import Env.spark.implicits._

            // process jdx files
            val filelist = Env.spark.createDataset( ("ls "+inputdir !!).split(raw"\n") )
            val expir_raw = filelist.map(readExpIRAndState).filter(_.isDefined).map(_.get)

            // remove data of bad structures(structures not in mid_structure)
            val mid_structure = MIDStruct.getOrCreate
            val join = expir_raw.joinWith(mid_structure,expir_raw("mid")===mid_structure("mid"))
            val expir = join.map(_._1)

            // output
            expir.show()
            expir.write.parquet(path)
        }

        def stats() = {
            val expir = getOrCreate
            val show_states = expir.groupBy(expir("state")).count().sort($"count".desc)
            show_states.show()
        }

    }
}
