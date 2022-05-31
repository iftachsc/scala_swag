// import sys.process._
// import scala.language.postfixOps
// import org.json4s._
// import org.json4s.native.JsonMethods._
// import org.w3c.dom.css.Counter
// import java.awt.event.KeyEvent
// import scala.concurrent.ExecutionContext
// import scala.concurrent.{ Await, Future }
// import scala.util.{Success,Failure}
// import scala.collection.mutable.Queue
// import java.util.concurrent.LinkedBlockingQueue
// import java.util.concurrent.ConcurrentLinkedQueue
// import java.util.concurrent.atomic.AtomicReference
// import scala.concurrent.duration.Duration
// import hw.hunters.io.SourceOperator
// import hw.hunters.io.StdoutSourceOperator
// import hw.hunters.io.WordEvent
// import hw.hunters.io.KeyByOperator
// import hw.hunters.io.WindowByKeyOperator


// object Main extends App {
//   implicit val ec = ExecutionContext.global

//   implicit val formats = DefaultFormats

//   var liftErrors = 0;

// //  val queue = Queue[WordEvent]
  
//   // val s = for {

//   //     a <- status.map(line => Option(WordEvent(line)))
//   //                .filter(_.nonEmpty)
//   //                .groupBy(event => event.get.key.get)
//   //                .foreach(println)
            
//   // } yield "a"

//   //lift phase

//   //assumptions:
//   //1. data is always a single word

//   //var queues = Map[String,ConcurrentLinkedQueue[WordEvent]]();

//   // for(x <- 0 to 3){
//   //   val ingest = Future {
//   //     val source = "/Users/yftach.shenbaum/Downloads/blackbox" lazyLines;
      
//   //     println("Starting producing from Thread")
//   //     source.map(line => Option(WordEvent(line)))
//   //           .filter(_.nonEmpty)
//   //           .filter(_.get.key.nonEmpty).flatten
//   //           .foreach(x => {
//   //               if(queues.get(x.key.get).isEmpty)
//   //                 queues += (x.key.get -> new java.util.concurrent.ConcurrentLinkedQueue[WordEvent])
//   //               queues.get(x.key.get).get.add(x)
//   //     })
      
//   //   }
//   //   Thread.sleep(1000) //jitter
//   // }
  
//   // val f = for {
//   //   _ <- StdoutSourceOperator[WordEvent](data => WordEvent(data),"/Users/yftach.shenbaum/Downloads/blackbox",4).stream
//   // } yield "success"
//   val source   = StdoutSourceOperator[WordEvent](data => WordEvent(data),"/Users/yftach.shenbaum/Downloads/blackbox",4).stream
//   val streams  = KeyByOperator(source).streams
//   val window   = WindowByKeyOperator(streams, windowSize = Duration("20 seconds"), slide = Duration("10 seconds"))
  
//   val stream = window.stream
  
//   while(true) {
//     while(Option(stream.peek).isEmpty) {
//       Thread.sleep(100)
//     }
//     println(stream.poll())
//   }


//   // val queue = keyStreams.get("baz").get
  
  



//   // val slide = 1000 //millis
//   // val range = 20000

//   //val slice = if (range % slide == 0) slide else range % slide
  

//   // val globalStartTS = System.currentTimeMillis()

//   // def combine(a: Map[String,Int], b: Map[String,Int]): Map[String,Int] = {
//   //     a ++ b.map{ case (k,v) => k -> (v + a.getOrElse(k,0)) }
//   // }

//   // def combine(a: WordEvent, b: Map[String,Int]): Map[String,Int] = {   
//   //   if(b.get(a.data).isEmpty)  //use case match
//   //     b + (a.data -> 1)
//   //   else
//   //     b + (a.data -> (b.get(a.data).get+ 1)) //data == word
//   // }
  

// // val f = Future {
// //       var partials = Queue[Map[String,Int]]().empty
// //       var sliceAggr = Map[String,Int]().empty
// //       val firstEventTS = queue.peek().timestamp * 1000
// //       val arrayPartials = Seq[Int]()
// //       var msgs = 0
// //       println(firstEventTS - globalStartTS)

// //       var sliceStart = firstEventTS //first slice by first item
      
// //       var event = queue.poll();
      
// //       while(true){
// //         //assuming here monotonically increasing TS and event-time rather than processing time
// //         while(event.timestamp*1000 < sliceStart + slide) {
// //           sliceAggr = combine(event, sliceAggr);

// //           event = queue.poll()
// //           msgs+=1;
// //           while(Option(event).isEmpty) {
// //             Thread.sleep(100) //wiating for messages
// //             event = queue.poll() 
// //           }
// //         }
        
// //         //println("slice: " + sliceStart + " - " + (sliceStart+slide) + ": " + msgs)
        
// //         msgs = 0;
// //         sliceStart += slice
// //         partials.addOne(sliceAggr)
// //         //println(partials)
// //         //trigger window
// //         var before = System.currentTimeMillis()
// //         if(partials.length > (range / slice)) {
// //           before = System.currentTimeMillis()
// //           println("window: "+ (partials.take(range / slide).reduce((l,r) => combine(l,r))))
// //           println("final agg took:" + (System.currentTimeMillis() - before ))
// //           partials.dequeue() //removing oldest slice after trigger window
// //         }
// //         sliceAggr = Map[String,Int]().empty
// //       }
// // } andThen {
// //     case Success(v) =>
// //       Thread.sleep(10000)
// //       println("The program waited patiently for this callback to finish.")
// //     case Failure(e) =>
// //       println(e)
// // }

// //Await.ready(f,Duration.Inf)

// println("Reached the end of the program")


//         // .groupBy(event => event.get.key.get)
//         // .take(4)
//         // .view.values.map(stream =>
//         //    Future{
//         //      //stream.foreach(println)
//         //      stream.take(2)
//         //      .foreach(println)
//         //      //Thread.sleep(3000)
//         //      println("finished in thread")
//         //    }
//         //  )
//         // .foreach(_.andThen{case Success(x) => println("x")})
        
        
        
//   //var partials : Array[]  




//   // status.map(line =>
//   //   try {
//   //         val s = Some(parse(line).extract[WordEvent]);
//   //   }
//   //   catch  {
//   //       case _: ParserUtil.ParseException => liftErrors+=1
//   //       case _: MappingException => liftErrors+=1;
//   //       case t: Throwable => println("Got some: " +t.getClass());throw t;
//   //   }
//   // )

// }





