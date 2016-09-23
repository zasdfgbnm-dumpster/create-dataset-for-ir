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

		def stats()

	}
	abstract class ProductTable[T] extends Table {

		type rowType = T

	}

	object TableManager {

		private val tables = Map[String,AnyRef]()

		def getOrCreate[T:Encoder](obj:ProductTable[T]):Dataset[T] = {
			val name = obj.getTableName
			if(!tables.contains(name)){
				val path = Env.tables+"/" + name
				if(!Files.exists(Paths.get(path))) {
					obj.create(path)
				}
				Env.spark.read.parquet(path).as[T]
			} else {
				tables(name).asInstanceOf[Dataset[T]]
			}
		}
	}

}
