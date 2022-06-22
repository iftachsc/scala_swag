package ai.hunters.homework.iftach.zio.challange

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
import state._

sealed trait Swag {
    type Slice  = Map[(String, String),Int]
    type WindowState = Slice

    def partialAggregationsByEventTime(stream: ZStream[Clock with Random, Nothing,WordEvent], 
                                        eventTimeExtractor: WordEvent => Long,      
                                         windowSize: Duration, slide: Duration, lateArrivalGrace: Duration,windowRef: Ref[Slice]): ZIO[Clock with Random with Console,IOException,Ref[WindowState]]

    def combineSlices(slice1: Slice, slice2: Slice): Slice
}

object LargeWindowsSwag extends Swag {
    def partialAggregationsByEventTime(stream: ZStream[Clock with Random, Nothing,WordEvent], 
                                        eventTimeExtractor: WordEvent => Long,      
                                         windowSize: Duration, slide: Duration, lateArrivalGrace: Duration,windowRef: Ref[Slice]) = ???
    
    def combineSlices(slice1: Slice, slice2: Slice): Slice = ???
}

object SimpleSwag extends Swag {
 
    def combineSlices(slice1: Slice, slice2: Slice): Slice =
        slice1 ++ slice2.map { 
            case (k,v) => k -> (v + slice1.getOrElse(k,0))
        }
    
    def partialAggregationsByEventTime(stream: ZStream[Clock with Random, Nothing,WordEvent], 
                                        eventTimeExtractor: WordEvent => Long, windowSize: Duration, 
                                        slide: Duration, lateArrivalGrace: Duration, windowRef: Ref[Slice]) = {
        
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

        
        
        val sliceSize = gcd(windowSize.toMillis, slide.toMillis).millis
        //deviding in a GCD so this have to result into whole number
        val numSlicesInWindow = (windowSize.toMillis / sliceSize.toMillis).toInt

        def computeSliceAndUpdateState(sliceEnd: Duration, slicesAggrState: FiberRef[Queue[Slice]]) = 
            //this pulls messages for the next slice and compute it. then adds it to the slices state. additionaly 
            for {
                sliceAggr <- computeNextSlice(sliceEnd).tap(slice => putStrLn(s"Handled slice: ${slice}"))
                            //when we filled the window with slices and need to start remove the oldest one on each insertion
                _         <- slicesAggrState.updateAndGet(_.appended(sliceAggr) match {
                                case q if(q.length > numSlicesInWindow) => q.dequeue._2
                                case q => q
                             })
            } yield ()
        
        def slideAndEmitWindow(slideStart: Duration, slicesAggrState: FiberRef[Queue[Slice]]) =
            for{
                //computing slide worth of slices
                _ <- ZIO.loop(slideStart)(_ < slideStart+slide,_ + sliceSize)(sliceStart =>
                        computeSliceAndUpdateState(sliceStart + sliceSize,slicesAggrState)
                )
                //if we filled first window with enowe have a completed a whole slide of  slices, we can compute a window
                window <-  slicesAggrState.get.map(queue => 
                            if (queue.length == numSlicesInWindow)
                                queue.iterator.fold(emptyStateMap)(combineSlices(_,_))
                            else
                                emptyStateMap
                            ).timed.flatMap({ //measurring computation of the window
                                    case (_, window) if(window.isEmpty) => ZIO.succeed(window)  //when the first window is still not available                                     
                                    case (time, window) => putStrLn(s"Window: ${window} [compute time: ${time.toMillis} millis]").as(window)
                                })
                _  <- windowRef.set(window)
            } yield ()

        for {
            _                 <- putStrLn(s"Slice Size: ${sliceSize.toSeconds} second/s").zipPar(putStrLn(s"#Slices in window: ${numSlicesInWindow}"))
            slicesAggrState   <- FiberRef.make(Queue[Slice]().empty)
            start             <- currentTime(TimeUnit.MILLISECONDS)
            _                 <- ZIO.loop(Duration.fromMillis(start))(_ => true,_ + slide)(slideStart => 
                                    slideAndEmitWindow(slideStart, slicesAggrState)).fork
      } yield windowRef
  }
}





