package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> false
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def compteur(chars: Array[Char], c: Int): Int =
      if chars.isEmpty then c else
        if chars.head == '(' then compteur(chars.tail, c + 1) 
        else if chars.head == ')' then
            if c >= 1 then compteur(chars.tail, c - 1) else -1
        else compteur(chars.tail, c)
    
    if compteur(chars, 0) == 0 then true else false
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int) : (Int, Int) = {
      if idx < until then chars(idx) match
        case '(' => traverse(idx + 1, until, arg1 + 1, Math.min(arg1 + 1, arg2))
        case ')' => traverse(idx + 1, until, arg1 - 1, Math.min(arg1 - 1, arg2))
        case _ => traverse(idx + 1, until, arg1, arg2)
      else (arg1, arg2)
    }

    def reduce(from: Int, until: Int) : (Int, Int) = {
      val s = until - from
      if s <= threshold then traverse(from, until, 0, 0) else
        val s2 = s / 2
        val ((x1, x2), (y1, y2)) = parallel(reduce(from, from + s2), reduce(from + s2, until))
        (x1 + y1, Math.min(x2, x1 + y2))
    }
    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
