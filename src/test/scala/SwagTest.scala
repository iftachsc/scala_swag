package ai.hunters.homework.iftach.zio.challange

import zio._
import zio.console._
import zio.test._
import zio.test.Assertion._
import zio.test.environment._
//import zio.test.magnolia._
import java.io.IOException
import zio.duration._
import java.util.concurrent.TimeUnit
import state._
import zio.clock._
import zio.stream._
import zio.random._
import zio.json._

object SwagSpec extends  DefaultRunnableSpec {
  type Slice  = Map[(String, String),Int]
  type WindowState = Slice
  
  def spec = suite("SwagSpec")(
    testM("Testing Swag windows correctnes") {
        val windowSize = 6.seconds
        val slide      = 3.seconds
       
      for {
        state <- getState
        blackbox <- ZIO.access[Has[ZStream[Clock with Random with Clock, Nothing, String]]](_.get)
        ref <- SimpleSwag.partialAggregationsByEventTime(
                    blackbox.map(_.fromJson[WordEvent]).collectRight, 
                    _.timestamp,
                    windowSize,
                    slide,
                    1.second,
                    state)
        _ <- TestClock.adjust(7.seconds)
        output <- TestConsole.output
        result <- ref.get
        } yield assert(result)(equalTo(Map(("baz","ipum") -> 7))) && 
                assertTrue(output.filter(_.startsWith("Handled slice")).length == 2) &&
                assertTrue(output.filter(_.startsWith("Window")).length == 1)        
    },

        testM("Testing Swag window results and slice count and window result correctness") {
        val windowSize = 20.seconds
        val slide      = 3.seconds
       
      for {
        state <- getState
        blackbox <- ZIO.access[Has[ZStream[Clock with Random with Clock, Nothing, String]]](_.get)
        ref <- SimpleSwag.partialAggregationsByEventTime(
                    blackbox.map(_.fromJson[WordEvent]).collectRight, 
                    _.timestamp,
                    windowSize,
                    slide,
                    1.second,
                    state)
        _ <- TestClock.adjust(22.seconds)
        output <- TestConsole.output
        result <- ref.get
        } yield assert(result)(equalTo(Map(("baz","ipum") -> 20))) && 
                assertTrue(output.filter(_.startsWith("Handled slice")).length == 21) &&
                assertTrue(output.filter(_.startsWith("Window")).length == 1)        
    },

    
    testM("Testing Combiner") {
      val slice1: Slice = Map(("type1","data1") -> 1, (("type2","data2") -> 2))
      val slice2: Slice = Map(("type1","data1") -> 1, (("type3","data2") -> 2))

      val combinedSlice = SimpleSwag.combineSlices(slice1, slice2)
      ZIO.succeed(assert(combinedSlice)(equalTo(Map(("type1","data1") -> 2, ("type2","data2") -> 2, ("type3","data2") -> 2))))
    },
  ).provideCustomLayer(State.live ++ DataGenTestLayer.blackbox)
}