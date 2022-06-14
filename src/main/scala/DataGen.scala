import zio.stream._
import zio.clock._
import zio.random._
import java.util.concurrent.TimeUnit
import zio._
import zio.duration._
import zio.json._

object DataGen {
  def blackbox =  {
    val event_types = Seq("baz", "dolor", "bar")
    val event_data = Seq("ipum", "amet", "shuki")
    val malformed = Seq("""{ "G'{úé¿—½£""","""{ "ð•±ÀA(Æ¸""")

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

    val malFormedGenerator = 
      for {
        r1  <- nextIntBetween(0, malformed.length)
      } yield malformed(r1)
    
    //generating 10% malformed data
    val eventGenerator = nextIntBetween(0,10).flatMap(x=> if(x == 1) malFormedGenerator else typedEventGenerator)
    
    ZStream.repeatEffectWith(eventGenerator, Schedule.spaced(20.milliseconds).jittered)
  }
}

case class WordEvent(timestamp: Long, event_type: String, data: String)

object WordEvent{
  implicit val decoder: JsonDecoder[WordEvent] = DeriveJsonDecoder.gen
}
