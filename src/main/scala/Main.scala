package ai.hunters.homework.iftach.zio.challange

import zio.console
import zio._
import java.time
import zio.stream._
import zio.duration
import java.util.concurrent.TimeUnit
import zio.json._
import zio.stream.ZStream.TerminationStrategy
import zio.clock._
import zio.duration._
import zio.random._
import java.io.IOException
import zio.console._

object ZioMain extends zio.App {
  //assumptions:
  //2. not handling grace periods for now (i.e. wait for current window size data even if it reached its end
  

  def run(args: List[String]) =
    source.exitCode
  
  val source = {
    //not handling invalid values for windowSize and slide e.g. 0.
    //when windowSize == slide windows degenerate to Thumbling
    val windowSize = 30.seconds
    val slide      = 3.seconds

    for {
      now  <- currentTime(TimeUnit.MILLISECONDS)
      state <- SimpleSwag.partialAggregationsByEventTime(
                  DataGen.blackbox.map(_.fromJson[WordEvent]).collectRight, 
                  _.timestamp,
                  windowSize,
                  slide,
                  1.second)
      _ <- Http4sServer.server(state)
      //).catchAll(_ => IO.fail("Something went wrong computing sliding window"))
    } yield ()
  }
}

 






