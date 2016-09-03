import scala.reflect.macros.blackbox.Context
import scala.language.experimental.macros
import scala.annotation.StaticAnnotation
import scala.reflect.runtime.universe._

package irms {
	object fgargs {
		def clsMacro(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
			import c.universe._
			val arglist = FunGrps.func_grps.map(TermName(_)).map(j=>q"$j:Boolean")
			val result = annottees.map(_.tree).toList match {
				case q"case class $name() { ..$body }" :: Nil => q"case class $name(..$arglist) { ..$body }"
			}
			c.Expr[Any](result)
		}
	}
	class fgargs extends StaticAnnotation {
		def macroTransform(annottees: Any*):Any = macro fgargs.clsMacro
	}

	object fgexpand {
		def expandMacro(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
			import c.universe._
			val len = FunGrps.func_grps.length
			val arglist = Range(0,len).map(j=>q"a($j)")
			val result = annottees.map(_.tree).toList match {
				case q"def apply(a:Array[Boolean]):$tpe = $obj" :: Nil =>
					 q"def apply(a:Array[Boolean]):$tpe = $obj(..$arglist)"
			}
			c.Expr[Any](result)
		}
	}
	class fgexpand extends StaticAnnotation {
		def macroTransform(annottees: Any*):Any = macro fgexpand.expandMacro
	}
}
