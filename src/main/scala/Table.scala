import java.nio.file.{Paths, Files}
import org.apache.spark.sql._
import scala.collection.mutable.Map

package irms {

	abstract class Table[T : Encoder] {

		import Env.spark.implicits._

		var ds:Option[Dataset[T]] = None

		val getTableName:String = {
			val raw = this.getClass.getName
			raw.split("\\$")(0).split("\\.").last
		}

		def create(path:String):Unit

		def getOrCreate:Dataset[T] = {

			if(ds==None){
				val path = Env.tables+"/" + getTableName
				if(!Files.exists(Paths.get(path))) {
					println("Table " + getTableName + " does not exist, now create.")
					create(path)
				}
				ds = Some(Env.spark.read.parquet(path).as[T])
			}
			ds.get
		}
	}

}
