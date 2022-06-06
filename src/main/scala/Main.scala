//import scala.concurrent.duration.Duration
import homework.hunters.io.operator._

import zio.console._
import zio.process._
import zio._
import java.time
import zio.stream._
import zio.duration.Duration
import java.util.concurrent.TimeUnit
import zio.json._
import zio.stream.ZStream.TerminationStrategy
import zio.clock._
import java.sql.Time

object ZioMain extends zio.App {
  //assumptions:
  //1. data is always a single word
  //2. not handling grace periods for now (i.e. wait for current window size data even if it reached its end
  //3. we handles late arrivals with grace period measured in milliseconds
  

  def run(args: List[String]) =
    source.exitCode
  
  // val myAppLogic = 
  //   for {
  //     fiber <-  Command("/Users/yftach.shenbaum/Downloads/blackbox").linesStream
  //     _       <- ZIO.sleep(duration.`package`.Duration.fromMillis(5000))
  //     _       <- process.kill
  //   } yield ()

  val paralelism = 1
  val command = Command("/Users/yftach.shenbaum/Downloads/blackbox")
  val source = for {
                now <- currentTime(TimeUnit.MILLISECONDS)
                fiber <- command.linesStream.merge(command.linesStream,TerminationStrategy.Both).merge(command.linesStream,TerminationStrategy.Both).merge(command.linesStream,TerminationStrategy.Both).merge(command.linesStream,TerminationStrategy.Both).merge(command.linesStream,TerminationStrategy.Both)
                        .map(x => x.fromJson[WordEvent]).collectRight
                            .takeWhile(_.timestamp < now + 3000)
                              .aggregateAsync(ZTransducer.foldLeft(0)((count,_) => count + 1))
                              .take(2)
                                .aggregateAsync(ZTransducer.foldLeft(0)((count,_) => count + 1))
                                .tap(x => 
                                  putStrLn(s"Element $x"))
                                  .runCollect.fork
                            
                _ <- putStrLn("its running!")
                _ <- fiber.join
              } yield ()
    
  
  
  


  // val command = "/Users/yftach.shenbaum/Downloads/blackbox"

  // val sourceOperator      = StdoutSourceOperator[WordEvent](fac = data => WordEvent(data), command, parallelism = 4)
  // //val mapOperator         = MapOperator[WordEvent,(Long, String, Int)](sourceOperator.stream, x => (x.timestamp, x.event_type + "-" + x.data,1))
  // val keyByOperator       = KeyByOperator[WordEvent](x => x.key, sourceOperator.stream, parallelism = 1)

  // val windowByKeyOperator = WindowByKeyOperator(keyByOperator.streams, windowSize = Duration("20 seconds"), slide = Duration("5 seconds"))
  
  
  // val sink                = StdOutSinkOperator(windowByKeyOperator.stream, block = false)   

  // while(true){
  //   println(windowByKeyOperator.queryState(("baz","dolor")))
  //   Thread.sleep(3000)
  // }
  // println("Reached the end of the program")

  
}

 






