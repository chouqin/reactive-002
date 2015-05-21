import calculator._

val exprs: Map[String, Signal[Expr]] = Map("a" -> Var {Literal(1)}, "b" -> Signal {Plus(Ref("a"), Literal(1))})
val values = Calculator.computeValues(exprs)

values("a")()
values("b")()

exprs("a")