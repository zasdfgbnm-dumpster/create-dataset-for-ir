import sys.process._
import scala.language.postfixOps
import scala.io.Source
import scala.collection.mutable
import scala.util.matching.Regex
import org.apache.spark.sql._
import org.apache.spark._

package irms {
    object CreateTheoreticalIR {

        private val sdf = "outputs/01/sdf_files"

        private case class fn_m_freq(mid:String,method:String,freqsformat:String,freqs:Array[(Double,Double)])

        private def read(filename:String):Option[fn_m_freq] = {
            val content = Source.fromFile(sdf+s"/$filename").mkString
            val pattern = raw"\> \<([\w\.]+)\>"
            var fieldcontents = content.split(pattern)
            val fieldnames = pattern.r.findAllIn(content).toArray
            val idxmethod = fieldnames.indexOf("> <METHOD>")
            val idxirfreq = fieldnames.indexOf("> <IR.FREQUENCIES>")
            if( idxmethod > 0 && idxirfreq > 0 ) {
                val method = fieldcontents(idxmethod+1).trim
                val irfreqstr = fieldcontents(idxirfreq+1).trim
                def line2tuple(str:String) = {
                    val a = str.trim.split(raw"\s+",2)
                    (a(0).toDouble,a(1).toDouble)
                }
                val irfreqlines = irfreqstr.split(raw"\n+")
                Some(new fn_m_freq(filename.split("\\.")(0),method,irfreqlines(0).trim,irfreqlines.tail.map(line2tuple)))
            } else
                None
        }

        def main(args: Array[String]): Unit = {
            val session = SparkSession.builder.appName("04_create_expir_table").getOrCreate()
            import session.implicits._

            // read sdf files
            val files = session.createDataset( (s"ls $sdf" !!).split(raw"\s+") )
            val data = files.map(read).filter(_.isDefined).map(_.get)
            data.groupBy(data("freqsformat")).count().sort($"count".desc).show()

            // replace mid with structure
            val mid_structure = session.read.parquet("outputs/03/mid_structure").as[MIDStruct]
            val join = data.joinWith(mid_structure,data("mid")===mid_structure("mid"))
            val table = join.map(j => new TheoreticalIR(smiles=j._2.smiles,method=j._1.method,freqs=j._1.freqs))

            // outputs
            table.show()
            table.groupBy(table("method")).count().sort($"count".desc).show()
            table.write.parquet("outputs/tables/thir")
        }
    }
}
