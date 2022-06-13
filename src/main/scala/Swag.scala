import zio.stream.ZStream._
import zio.stream.ZStream
import zio.clock._
import zio.random._
import java.util.concurrent.TimeUnit
import zio._
import zio.duration._
import zio.console._
import scala.collection.immutable.Queue
import java.{util => ju}
import java.io.IOException


sealed trait Swag {
    def partialAggregationsByEventTime(stream: ZStream[Clock with Random, Nothing,WordEvent], 
                                        eventTimeExtractor: WordEvent => Long,      
                                         windowSize: Duration, slide: Duration, lateArrivalGrace: Duration): ZIO[Clock with Random with Console,IOException,Ref[Map[(String,String),Int]]]

}

object LargeWindowsSwag extends Swag {
    def partialAggregationsByEventTime(stream: ZStream[Clock with Random, Nothing,WordEvent], 
                                        eventTimeExtractor: WordEvent => Long,      
                                         windowSize: Duration, slide: Duration, lateArrivalGrace: Duration) = ???
}

object SimpleSwag extends Swag {
 
    def partialAggregationsByEventTime(stream: ZStream[Clock with Random, Nothing,WordEvent], 
                                        eventTimeExtractor: WordEvent => Long,      
                                         windowSize: Duration, slide: Duration, lateArrivalGrace: Duration) = {
    
        def emptyStateMap = Map[(String,String),Int]()

        def computeNextSlice(sliceEndTimestamp: Duration) = for {
            sliceAggregation <- stream
                .takeWhile(eventTimeExtractor(_).milliseconds <= sliceEndTimestamp)//.tap(x => putStrLn(s"Count $x")) 
                    .groupByKey(x => (x.event_type,x.data)) {
                        case(k, s) => 
                            ZStream.fromEffect(s.runCollect.map(l => k -> l.size))
                      }
                     .fold(emptyStateMap)(_ + _) //merging all key specific 
        } yield sliceAggregation

        def combineSlices(slice1: Map[(String,String),Int], slice2: Map[(String,String),Int]): Map[(String,String),Int] = {
            slice1 ++ slice2.map { 
                case (k,v) => k -> (v + slice1.getOrElse(k,0))
            }
        }

        val sliceSize = windowSize.toMillis % slide.toMillis match {
            case 0        => slide
            case leftOver => leftOver.millis
        }

        def computeSliceAndUpdateState(sliceEnd: Duration, slicesAggrState: Ref[Queue[Map[(String,String),Int]]], windowSizeInSlices: Long) = 
            for {
                sliceAggr    <- computeNextSlice(sliceEnd).tap(x => putStrLn("slice: "+ x.toString))
                newStateVal  <- slicesAggrState.updateAndGet(_.appended(sliceAggr))
                recentWindow <- if(newStateVal.iterator.length == windowSizeInSlices) 
                                    //we have a completed window slices wise, we can compute it now
                                    //since this is the only fiber that modifies the state we can now 
                                    //remove oldest slice from the state instead during the previous update
                                    //using modify which is more complicated
                                    ZIO.succeed(newStateVal.iterator.fold(emptyStateMap)(combineSlices(_,_)))
                                        .zipLeft(slicesAggrState.update(_.dequeue._2))
                                else
                                    ZIO.succeed(emptyStateMap)
                //_ <-  recentWindowState.set(recentWindow)      
            } yield recentWindow

        for {
            numSlicesInWindow <- ZIO.succeed((windowSize.toMillis / sliceSize.toMillis).toInt)
            slicesAggrState   <- Ref.make(Queue[Map[(String,String),Int]]().empty)
            recentWindowState <- Ref.make(emptyStateMap)
            start             <- currentTime(TimeUnit.MILLISECONDS)
            _                 <- ZIO.loop(Duration.fromMillis(start))(_ => true,_ + slide)(
                                    //this pulls messages for the next slice and compute it. then adds it to the slices state. additionaly 
                                    //returns new compuuted window if we have enough slices
                                    computeSliceAndUpdateState(_,slicesAggrState, numSlicesInWindow).flatMap(recentWindowState.set(_))
                                 ).fork
            _ <- (recentWindowState.get.flatMap(x => putStrLn("window: "+ x.toString)) repeat Schedule.spaced(2.seconds)).fork
      } yield recentWindowState
  }
}





