package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Signal {
      val va = a()
      val vb = b()
      val vc = c()
      vb * vb - 4 * va * vc
    }
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    Signal {
      val va: Double = a()
      val vb: Double = b()
      val vd: Double = delta()
      if (vd < 0) {
        Set()
      } else {
        Set((-vb + math.sqrt(vd)) / (2 * va), (-vb - math.sqrt(vd)) / (2 * va))
      }
    }
  }
}
