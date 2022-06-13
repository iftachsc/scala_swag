
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
  //1. data is always a single word
  //2. not handling grace periods for now (i.e. wait for current window size data even if it reached its end
  //3. we handles late arrivals with grace period measured in milliseconds
  

  def run(args: List[String]) =
    source.exitCode
  
  val source = {
    val windowSize = 10.seconds
    val slide      = 2.seconds

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

 






