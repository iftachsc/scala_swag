package ai.hunters.homework.iftach.zio.challange

import zio.stream._
import zio.clock._
import zio.random._
import java.util.concurrent.TimeUnit
import zio._
import zio.duration._
import zio.test.environment.TestClock
import zio.console._

//much simpler blackbox with single event type and data emitting messages on a fixed interval
object DataGenTestLayer {
   val blackbox =  {
    val event_types = Seq("baz")
    val event_data = Seq("ipum")

    val typedEventGenerator =
      for {
        now <- currentTime(TimeUnit.MILLISECONDS)
        r1  <- nextIntBetween(0, event_types.length)
        r2  <- nextIntBetween(0, event_data.length)
      } yield s"""{ 
                    "timestamp": $now,
                    "event_type": "${event_types(r1)}", 
                    "data": "${event_data(r2)}"
                  }"""
    
    //generating 10% malformed data
    val eventGenerator = typedEventGenerator
    
    ZLayer.succeed(ZStream.repeatEffectWith(eventGenerator, Schedule.fixed(1.second)))
  }
}
