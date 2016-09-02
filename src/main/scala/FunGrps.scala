import scala.io.Source

package irms {

	object FunGrps {

		val func_grps:Seq[String] = {
			val stream = getClass.getResourceAsStream("/readme.txt")
			val lines = Source.fromInputStream(stream).getLines.toList
			val uncomment = lines.map(_.mkString.split("//")(0).trim)
			val fields = uncomment.map(_.split("\t"))
			val selected = fields.filter(_.length==2)
			selected.map(_(1).trim)
		}
		
	}

}
