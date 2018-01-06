package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var count=0
    var i=0
    while(i<chars.length){
      
      if(chars(i)=='(')
          count=count+1
      else if(chars(i)==')')
          count=count-1
          
      if(count<0)
        return false
        
      i=i+1
    }
    
    count==0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {
    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int,Int) = {
      var i=idx
      var l=0
      var r=0
      while(i<until){
        
        if(chars(i)=='(')
          l=l+1
        else if(chars(i)==')')
          if(l>0)
            l=l-1
          else r=r+1
         
        i=i+1
      }
          
      (l,r)
    }

    def reduce(from: Int, until: Int): (Int,Int) = {
      if(until-from<=threshold)
          traverse(from,until,0,0)
      else{
        val m=from+(until-from)/2
        val (l,r)=parallel(reduce(from,m), reduce(m,until))
        if(l._1>r._2)
          (r._1+(l._1-r._2),l._2)
        else (r._1,l._2+(r._2-l._1))
      }
      
    }

    reduce(0, chars.length) == (0,0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
