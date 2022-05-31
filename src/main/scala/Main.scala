import scala.concurrent.duration.Duration
import homework.hunters.io.operator._

object Main extends App {
  //assumptions:
  //1. data is always a single word
  //2. not handling grace periods for now (i.e. wait for current window size data even if it reached its end

  val command = "/Users/yftach.shenbaum/Downloads/blackbox"

  val sourceOperator      = StdoutSourceOperator[WordEvent](fac = data => WordEvent(data), command, parallelism = 2)
  //val mapOperator         = MapOperator[WordEvent,(Long, String, Int)](sourceOperator.stream, x => (x.timestamp, x.event_type + "-" + x.data,1))
  //val keyByOperator       = KeyByOperator[(Long, String, Int)](x => x._2, mapOperator.stream, parallelism = 1)
  val keyByOperator       = KeyByOperator[WordEvent](x => x.key.get, sourceOperator.stream, parallelism = 1)

  val windowByKeyOperator = WindowByKeyOperator(keyByOperator.streams, windowSize = Duration("20 seconds"), slide = Duration("5 seconds"))
  val sink                = StdOutSinkOperator(windowByKeyOperator.stream, block = true)   

  println("Reached the end of the program")

  
}

 






