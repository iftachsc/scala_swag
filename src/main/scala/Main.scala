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
import state._

object ZioMain extends zio.App {
  //assumptions:
  //2. not handling grace periods for now (i.e. wait for current window size data even if it reached its end

  val program = {
    //not handling invalid values for windowSize and slide e.g. 0.
    //when windowSize == slide windows degenerate to Thumbling
    val windowSize = 30.seconds
    val slide      = 3.seconds

    val stateLayer = State.live
    
    for {
      state <- getState
      blackbox <- ZIO.access[Has[ZStream[Clock with Random with Clock, Nothing, String]]](_.get)
      _ <- SimpleSwag.partialAggregationsByEventTime(
                  blackbox.map(_.fromJson[WordEvent]).collectRight, 
                  _.timestamp,
                  windowSize,
                  slide,
                  1.second,
                  state)
          .zipPar(Http4sServer.server(state))
    } yield ()
  }

  def run(args: List[String]) =
    program.provideCustomLayer(State.live ++ DataGen.blackbox).exitCode
}

 






