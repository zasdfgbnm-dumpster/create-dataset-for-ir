import scala.io.Source
import scala.reflect.macros.blackbox.Context
import scala.language.experimental.macros
import scala.annotation.StaticAnnotation
import scala.reflect.runtime.universe._

package irms {

	object FunGrps {

		val func_grps:Seq[String] = {
			val stream = getClass.getResourceAsStream("/FunctionalGroups.txt")
			val lines = Source.fromInputStream(stream).getLines.toList
			val uncomment = lines.map(_.mkString.split("//")(0).trim)
			val fields = uncomment.map(_.split("\t"))
			val selected = fields.filter(_.length==2)
			selected.map(_(1).trim)
		}

		def impl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
			import c.universe._
			val arglist = FunGrps.func_grps.map(TermName(_)).map(j=>q"$j:Boolean")
			val result = annottees.map(_.tree).toList match {
				case q"case class $name() { ..$body }" :: Nil => q"case class $name(..$arglist) { ..$body }"
			}
			c.Expr[Any](result)
		}

	}

	class fgargs extends StaticAnnotation {
		def macroTransform(annottees: Any*) = macro FunGrps.impl
	}
}
