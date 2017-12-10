package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
     
    Signal{
       val _a=a()
       val _b=b()
       val _c=c()
      _b*_b-4*_a*_c
      }
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
      
     Signal{
       val _a=a()
       val _b=b()
       val _c=c()
       val _d=delta()
       
       if(_d>=0)
         Set((-_b-Math.sqrt(_d))/(2*_a),
             (-_b+Math.sqrt(_d))/(2*_a))
       else 
         Set()
     }
  }
}
