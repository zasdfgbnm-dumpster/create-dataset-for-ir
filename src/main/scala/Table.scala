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

		val is_large = false;

	}

	object TableManager {

		private val tables = Map[String,DataFrame]()

		def getOrCreate(obj:Table):DataFrame = {
			val name = obj.getTableName
			if(!tables.contains(name)){
				val path = (if(obj.is_large) Env.large_tables else Env.tables) + "/" + name
				if(!Files.exists(Paths.get(path))) {
					obj.create(path)
				}
				Env.spark.read.parquet(path)
			} else {
				tables(name)
			}
		}
	}

}
