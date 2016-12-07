import java.nio.file.{Paths, Files}
import org.apache.spark.sql._
import scala.collection.mutable.Map

package irms {

	abstract class Table {

		val getTableName:String = {
			val raw = this.getClass.getName
			raw.split("\\$")(0).split("\\.").last
		}

		def create(path:String):Unit

	}

	object TableManager {

		private val tables = Map[String,AnyRef]()

		def getOrCreate(obj:Table):DataFrame = {
			val name = obj.getTableName
			if(!tables.contains(name)){
				val path = Env.tables+"/" + name
				if(!Files.exists(Paths.get(path))) {
					obj.create(path)
				}
				Env.spark.read.parquet(path)
			} else {
				tables(name).asInstanceOf[DataFrame]
			}
		}
	}

}
