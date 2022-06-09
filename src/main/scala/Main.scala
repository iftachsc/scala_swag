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
import zio.duration._
import zio.random._
import java.io.IOException


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

  

  
  
  
  
  val source = for {
     now <- currentTime(TimeUnit.MILLISECONDS)
     _ <- putStrLn(now.toString)
     //_ <- SimpleSwag partialAggregationsByEventTime(DataGen.blackbox.map(x => x.fromJson[WordEvent]).collectRight,4.seconds,now.millis, 1.second)
     _ <- SimpleSwag.partialAggregationsByEventTime(DataGen.blackbox.map(x => x.fromJson[WordEvent]).collectRight, _.timestamp ,4.seconds, 1.second)
  } yield ()

  
  // val source = for {
  //               grace <- ZIO.succeed(2.seconds)
  //               slide <- ZIO.succeed(6.seconds)
  //               now <- currentTime(TimeUnit.SECONDS).tap(x => putStrLn(x.toString))
  //               //fiber <- (1 to volumeScale).foldLeft(command.linesStream)((x,_) => x.merge(command.linesStream,TerminationStrategy.Both))
  //               fiber     <- blackbox
  //                             .tap(x => putStrLn(s"Count $x"))
  //                             .map(x => x.fromJson[WordEvent]).collectRight
  //                             .runCollect

  //               _ <- partialAggregationsByEventTime(fiber, 3.seconds, now.seconds)
  //                               // .takeWhile(_.timestamp <= now + slide.getSeconds)
  //                               // .aggregateAsyncWithin(ZTransducer.foldLeft(0)((x,_) => x + 1)
  //                               //   ,Schedule.fixed(slide + grace))//limitting grace time for late arrivals
                                
  //                             //.take(2)
  //                              // .aggregateAsync(ZTransducer.foldLeft(0)(_ + 1))
  //                               // .tap(x => 
  //                               //   putStrLn(s"Count $x"))
  //                                 //.runHead
  //               _ <- putStrLn("its running!")
  //               // _ <- fiber.join
  //             } yield ()
    
  
  
  


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

 






