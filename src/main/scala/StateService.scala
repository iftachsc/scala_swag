package ai.hunters.homework.iftach.zio.challange

import zio._
import zio.clock._
import zio.console._
import zio.duration.Duration._
import java.io.IOException

object state {
  type State = Has[State.Service]
    
  object State {
    
    trait Service {
      def updateAndGet(update: Map[(String,String),Int] => Map[(String,String),Int]): UIO[Map[(String,String),Int]]
      
    }

    val live: Layer[Nothing, Has[Service]] = ZLayer.succeed {
      new Service {
        val ref = Ref.make(Map[(String,String),Int]().empty)

        def updateAndGet(update: Map[(String,String),Int] => Map[(String,String),Int]) = 
            ref.flatMap(_.updateAndGet(update(_)))

      }
    }
  }

  def updateAndGet(update: Map[(String,String),Int] => Map[(String,String),Int]): URIO[State, Map[(String,String),Int]] =
    ZIO.accessM(_.get.updateAndGet(update))
}
