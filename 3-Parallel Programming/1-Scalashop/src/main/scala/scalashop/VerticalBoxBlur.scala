package scalashop

import org.scalameter._
import common._

object VerticalBoxBlurRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 5,
    Key.exec.maxWarmupRuns -> 10,
    Key.exec.benchRuns -> 10,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val radius = 3
    val width = 1920
    val height = 1080
    val src = new Img(width, height)
    val dst = new Img(width, height)
    val seqtime = standardConfig measure {
      VerticalBoxBlur.blur(src, dst, 0, width, radius)
    }
    println(s"sequential blur time: $seqtime ms")

    val numTasks = 32
    val partime = standardConfig measure {
      VerticalBoxBlur.parBlur(src, dst, numTasks, radius)
    }
    println(s"fork/join blur time: $partime ms")
    println(s"speedup: ${seqtime / partime}")
  }

}

/** A simple, trivially parallelizable computation. */
object VerticalBoxBlur {

  /** Blurs the columns of the source image `src` into the destination image
   *  `dst`, starting with `from` and ending with `end` (non-inclusive).
   *
   *  Within each column, `blur` traverses the pixels by going from top to
   *  bottom.
   */
  def blur(src: Img, dst: Img, from: Int, end: Int, radius: Int): Unit = {
    // TODO implement this method using the `boxBlurKernel` method
    var x=from
    while(x< clamp(end,0,src.width)){
        var y=0
        while(y < src.height){
          dst.update(x,y,boxBlurKernel(src,x,y,radius))
          y=y+1
        }
        x=x+1
    }        
  }

  /** Blurs the columns of the source image in parallel using `numTasks` tasks.
   *
   *  Parallelization is done by stripping the source image `src` into
   *  `numTasks` separate strips, where each strip is composed of some number of
   *  columns.
   */
  def parBlur(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    // TODO implement using the `task` construct and the `blur` method
    val width_strip=clamp(src.width/numTasks,1,src.width)
    val rest=src.width%numTasks
    
    //Compute de the list of the beginnings of each strip
    val beginnings= (0 to src.width by width_strip).toList
    
    //Correct the beginnings of the strips to distribute the remaining pixels
    val correction=(0 to rest-1).toList:::List.fill(beginnings.size-rest)(rest)
    val corrected_beginnings=beginnings.zip(correction).map( t => t._1+t._2  )
    
    //Construct the tuples containing the beginnings and ends of each strip
    val strips=corrected_beginnings.zip(corrected_beginnings.tail)

    val tasks=strips.map( strip => task( blur(src,
                                              dst,
                                              strip._1,
                                              strip._2,
                                              radius) )
                        )              
    tasks.foreach(_.join)
  }

}
