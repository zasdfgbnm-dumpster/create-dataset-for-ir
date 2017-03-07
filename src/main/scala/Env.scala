import java.nio.file.{Paths, Files}
import org.apache.spark.sql._
import org.apache.spark._

package irms {

	object Env {

		val ishpg = Files.exists(Paths.get("/ufrc/roitberg/qasdfgtyuiop"))

		// paths
		val workspace = if(ishpg) "/ufrc/roitberg/qasdfgtyuiop" else "/home/gaoxiang/MEGA"
		val large_workspace = "/mnt/data/gaoxiang"
		val raw = workspace + "/raw"
		val large_raw = large_workspace + "/raw"
		val tables = workspace + "/tables"
		val large_tables = large_workspace + "/tables"
		val bin = workspace + "/bin"
		val tmp = workspace + "/tmp"
		val data = workspace + "/data"

		// command to run python
		val pycmd = if(ishpg) bin+"/anaconda3/envs/my-rdkit-env/bin/python" else "python"

		// spark session
		def spark = SparkSession.builder.getOrCreate()
	}

}
