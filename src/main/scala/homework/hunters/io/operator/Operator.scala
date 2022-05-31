package homework.hunters.io.operator

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.{Future,ExecutionContext}
import sys.process._
import scala.language.postfixOps
import org.json4s._
import org.json4s.native.JsonMethods._
import scala.collection.concurrent.{Map => ConcurrentMap,TrieMap}
import scala.collection.immutable.{Map => ImmutableMap}
import scala.concurrent.{Await}
import scala.util.{Success,Failure}
import scala.concurrent.duration._
import util.control.Breaks._
import scala.collection.mutable.Queue
import scala.collection.immutable.Seq

trait Operator {
    // def map[A,B](mapFunction: A => B) : MapOperator[A,B] = {
    //     MapOperator(this.stream, mapFunction)
    // }
}
//sealed trait SourceOperator extends Operator


//assuming communication between Operators are via thread-safe ConcurrentLinkedQueues
case class StdoutSourceOperator[A<:StreamItem](val fac: String => A, command: String, parallelism: Int) extends Operator {
    implicit val ec = ExecutionContext.global

    def stream : ConcurrentLinkedQueue[A] = {
        val sink = new ConcurrentLinkedQueue[A] 
        for(x <- 0 to parallelism - 1){
            Future {
                val source = command lazyLines;
                println("Starting producing from Thread")
                source.map(line => Option(fac(line))) //Options will be empty for failed deserizlied WordEvents
                        .filter(_.nonEmpty) //discarding garbage elements
                        .filter(_.get.key.nonEmpty).flatten //keeping only messges with keys 
                        //.map(x => {println(x);x})
                        .foreach(d => sink.add(d))
            }
            Thread.sleep(1000) //jitter
        }
        sink
    }   
}

case class StdOutSinkOperator[A](stream: ConcurrentLinkedQueue[A], block: Boolean = true) extends Operator {  
    implicit val ec = ExecutionContext.global

    val f = Future {
        while(true) {
            while(Option(stream.peek).isEmpty) {
            Thread.sleep(100)
            }
            println(stream.poll())
        }
    }.andThen {
      case Success(v) =>
        Thread.sleep(10000)
        println("The program waited patiently for this callback to finish.")
      case Failure(e) =>
        println(e)
    }
    if(block)
        Await.ready(f, Duration.Inf)

}

case class KeyByOperator[A](keyBy: A => String, source: ConcurrentLinkedQueue[A], parallelism: Int = 1) extends Operator {
    implicit val ec = ExecutionContext.global

    def streams : Seq[ConcurrentLinkedQueue[A]] = {
    
        val queues: Seq[ConcurrentLinkedQueue[A]] = (1 to parallelism).map(_ => new ConcurrentLinkedQueue[A])
        //val queues = new TrieMap[String, ConcurrentLinkedQueue[A]]
        val f = Future{
            while(true) {
                breakable {
                    val item: Option[A] = Option(source.poll()) //null if Queue has no messages
                    if(item.isEmpty) {
                        Thread.sleep(100);
                        break;
                    }
                    //val key = item.get.key.get  //replace with keyBy(item)
                    val key = keyBy(item.get)
                    //queues.getOrElseUpdate(key.asInstanceOf[String], new ConcurrentLinkedQueue[A]).add(item.get)
                    queues(key.hashCode % parallelism).add(item.get)
                }
            }    
        }.andThen {
            case Success(v) =>
                Thread.sleep(10000)
                println("The program waited patiently for this callback to finish.")
            case Failure(e) =>
                println(e)
        }
        queues;
    }
}

case class MapOperator[A<:StreamItem,B](source: ConcurrentLinkedQueue[A], map: A => B) extends Operator {
    implicit val ec = ExecutionContext.global

    val sink = new ConcurrentLinkedQueue[B]

    def stream : ConcurrentLinkedQueue[B] = {
        //val queues: Map[String,ConcurrentLinkedQueue[A]] = new TrieMap()
        val f = Future{
            while(true) {
                breakable {
                    val item: Option[A] = Option(source.poll()) //null if Queue has no messages
                    if(item.isEmpty) {
                        Thread.sleep(500);
                        break;
                    }
                    sink.add(map(item.get))
                }
            }    
        }.andThen {
            case Success(v) =>
                Thread.sleep(10000)
                println("The program waited patiently for this callback to finish.")
            case Failure(e) =>
                println(e)
        }
        sink
    }

}

case class WindowByKeyOperator[A<:StreamItem,B](keyStreams : Seq[ConcurrentLinkedQueue[A]],/*aggr: (A,A) => B,*/ windowSize: Duration, slide: Duration) extends Operator {
    implicit val ec = ExecutionContext.global
    var state: ConcurrentMap[(String,String),Int] = new TrieMap[(String,String),Int] //e.g. baz -> (ipum,4)
    
    //assuming here key is string always
    //var state: Map[String,B] = new TrieMap()

    // def combine(a: Map[String,Int], b: Map[String,Int]): Map[String,Int] = {
    //   a ++ b.map{ case (k,v) => k -> (v + a.getOrElse(k,0)) }
    // }

    // def combine(a: WordEvent, b: Map[String,Int]): Map[String,Int] = {  
        
    //     if(b.get(a.data).isEmpty)  //use case match
    //         b + (a.data -> 1)
    //     else
    //         b + (a.data -> (b.get(a.data).get + 1))
    // }
    def queryState(key: (String,String)): Int = {

    }

    def combineSlices(a: Map[(String,String),Int], b: Map[(String,String),Int]): Map[(String,String),Int] = {  
        a ++ b.map{ case (k,v) => k -> (v + a.getOrElse(k,0)) }
    }

    def aggregateSlice(buffer: Seq[A]) = {
        buffer.groupBy(a => (a.key,a.data)).map(kv => (kv._1,kv._2.size))
    }
    
    def stream = {
        val sink: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]
        println("in WindowByKeyOperator")
        
            Future {
                val queue = keyStreams(0)
                //println("WindowByKey Thread !!!")
                val sliceSize = if (windowSize.toMillis % slide.toMillis == 0) slide else Duration(windowSize.toMillis % slide.toMillis, MILLISECONDS)
                var buffer = Queue[A]().empty
                
                var partials = Queue[ImmutableMap[(String,String),Int]]().empty
                var sliceAggr = Map[String,Map[String,Int]]().empty
                val firstEventTS = queue.peek().timestampMillis

                var msgs = 0
                //println(firstEventTS - globalStartTS)

                var sliceStart = firstEventTS //first window/slice starting at first item
                
                while(true){
                    var gracePeriodFinished = false
                    val graceMillis = Duration("1 second")
                    
                    var event = queue.poll();
                    while(Option(event).isEmpty) {
                        Thread.sleep(100) //wiating for messages
                        event = queue.poll() 
                    }
                    //assuming here monotonically increasing TS and event-time rather than processing time
                    while(event.timestampMillis < sliceStart + slide.toMillis && !gracePeriodFinished) {
                        //sliceAggr = combine(event.asInstanceOf[WordEvent], sliceAggr);
                        //sliceAggr ++ event.key.get -> combine(event.asInstanceOf[WordEvent], sliceAggr.get(event.key.get).get)
                        buffer.addOne(event)
                        event = queue.poll()
                        while(Option(event).isEmpty) {
                            Thread.sleep(100) //wiating for messages
                            event = queue.poll() 
                        }
                        msgs+=1;
                        
                    }
                    
                    //println("slice: " + sliceStart + " - " + (sliceStart+slide) + ": " + msgs)
                    
                    msgs = 0;
                    sliceStart += sliceSize.toMillis
                    
                    partials.addOne(aggregateSlice(buffer.toSeq))
                    buffer.clear
                    
                    
                    //println(partials)
                    //trigger window
                    var before = null.asInstanceOf[Long]
                    if(partials.length > windowSize / sliceSize) {
                        before = System.currentTimeMillis()
                        
                        //println("window: "+ (partials.take((windowSize / slide).intValue).reduce((l,r) => combine(l,r))))
                        //println(key+ "  @@@@")
                        val windowAggr = partials.take((windowSize / slide).intValue).reduce((l,r) => combineSlices(l,r))
                        sink.add("window: " + windowAggr
                                            + " [final aggr took:" + (System.currentTimeMillis() - before ) + " millies, latency: "
                                            + Duration(System.currentTimeMillis - event.timestampMillis, MILLISECONDS)
                                            + "]")
                        
                        partials.dequeue() //removing oldest slice after window is triggered
                    }
                    sliceAggr = Map[String,Map[String,Int]]().empty
                }
            } andThen {
                case Success(v) =>
                Thread.sleep(10000)
                println("The program waited patiently for this callback to finish.")
                case Failure(e) =>
                println("future failed")
                println(e)
            }
        
        sink
    }
    
}

sealed trait StreamItem {
    //assuming key always exists in a StreamItem
    def key: String

    def data: String

    def timestamp: Long

    def timestampMillis: Long
}
  
case class WordEvent(timestamp: Long, event_type: String, data: String) extends StreamItem {
  implicit val formats = DefaultFormats

  def key: String = {
    event_type
  }

  def timestampMillis: Long = {
      timestamp * 1000
  }
}

object WordEvent{
  def apply(s: String) : WordEvent = {
    implicit val formats = DefaultFormats

    try {
        parse(s).extract[WordEvent];
      }
      catch  {
          case _: ParserUtil.ParseException => null.asInstanceOf[WordEvent];
          case _: MappingException => null.asInstanceOf[WordEvent];
          case t: Throwable => println("Got some: " +t.getClass());throw t;
      } 
    }
}

