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
    type Slice  = Map[(String, String),Int]
    type WindowState = Slice

    def partialAggregationsByEventTime(stream: ZStream[Clock with Random, Nothing,WordEvent], 
                                        eventTimeExtractor: WordEvent => Long,      
                                         windowSize: Duration, slide: Duration, lateArrivalGrace: Duration): ZIO[Clock with Random with Console,IOException,Ref[Slice]]

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
        
        val emptyStateMap = Map[(String,String),Int]()

        def gcd(a: Long,b: Long): Long = 
            if(b ==0) a else gcd(b, a%b)
        
        def computeNextSlice(sliceEndTimestamp: Duration) = for {
            sliceAggregation <- stream
                .takeWhile(eventTimeExtractor(_).milliseconds <= sliceEndTimestamp)//.tap(x => putStrLn(s"Count $x")) 
                    .groupByKey(x => (x.event_type,x.data)) {
                        case(k, s) => 
                            ZStream.fromEffect(s.runCollect.map(l => k -> l.size))
                      }
                     .fold(emptyStateMap)(_ + _) //merging all key specific 
        } yield sliceAggregation

        def combineSlices(slice1: Slice, slice2: Slice): Slice = {
            slice1 ++ slice2.map { 
                case (k,v) => k -> (v + slice1.getOrElse(k,0))
            }
        }

        val sliceSize = gcd(windowSize.toMillis, slide.toMillis).millis
        //deviding in a GCD so this have to result into whole number
        val numSlicesInWindow = (windowSize.toMillis / sliceSize.toMillis).toInt

        def computeSliceAndUpdateState(sliceEnd: Duration, slicesAggrState: Ref[Queue[Slice]]) = 
            //this pulls messages for the next slice and compute it. then adds it to the slices state. additionaly 
            for {
                sliceAggr <- computeNextSlice(sliceEnd).tap(x => putStrLn("Handled slice: "+ x.toString))
                            //when we filled the window with slices and need to start remove the oldest one on each insertion
                state <- slicesAggrState.updateAndGet(_.appended(sliceAggr) match {
                                case q if(q.length > numSlicesInWindow) => q.dequeue._2
                                case q => q
                             })
                _     <- putStrLn(s" num slices in state: ${state.length}")
            } yield ()
        
        def computeSlideAndUpdateState(slideStart: Duration, slicesAggrState: Ref[Queue[Slice]], 
                recentWindowState: Ref[WindowState]) =
            for{
                _ <- ZIO.loop(slideStart)(_ < slideStart+slide,_ + sliceSize)(sliceStart =>
                        computeSliceAndUpdateState(sliceStart + sliceSize,slicesAggrState)
                )
                //if we filled first window with enowe have a completed a whole slide of  slices, we can compute a window
                window <-  slicesAggrState.get.map(queue => 
                            if (queue.length == numSlicesInWindow)
                                queue.iterator.fold(emptyStateMap)(combineSlices(_,_))
                            else
                                emptyStateMap).timed.flatMap({
                                    case (time, effect) => 
                                        putStrLn(s"Window: ${effect} [compute time: ${time.toMillis} millis]").as(effect)
                                })
                _  <- recentWindowState.set(window)
            } yield ()

        for {
            _                 <- putStrLn(s"Slice Size: ${sliceSize.toSeconds} second/s").zip(putStrLn(s"numSlicesInWindow: ${numSlicesInWindow}"))
            slicesAggrState   <- Ref.make(Queue[Slice]().empty)
            recentWindowState <- Ref.make(emptyStateMap)
            start             <- currentTime(TimeUnit.MILLISECONDS)
            _                 <- ZIO.loop(Duration.fromMillis(start))(_ => true,_ + slide)(slideStart => 
                                    computeSlideAndUpdateState(slideStart, slicesAggrState, recentWindowState)).fork
      } yield recentWindowState
  }
}





