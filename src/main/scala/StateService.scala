package ai.hunters.homework.iftach.zio.challange

import zio._
import zio.clock._
import zio.console._
import zio.duration.Duration._
import java.io.IOException


object state {    

  type State = Has[State.Service]
  type Slice  = Map[(String, String),Int]
  val emptyStateMap = Map[(String,String),Int]()

  object State {
    
    trait Service {
      def updateAndGet(update: Slice => Slice): UIO[Slice]
      def getState: UIO[Ref[Slice]]
    }

    val live: Layer[Nothing, Has[Service]] = ZLayer.succeed {
      new Service {
        val state = Ref.make(emptyStateMap)

        override def updateAndGet(update: Map[(String,String),Int] => Map[(String,String),Int]) = 
            state.flatMap(_.updateAndGet(update(_)))

        override def getState = state
      }
    }
  }

  def updateAndGet(update: Slice => Slice): URIO[State, Slice] =
      ZIO.accessM(_.get.updateAndGet(update))

  def getState: URIO[State, Ref[Slice]] = 
      ZIO.accessM(_.get.getState)
  
}

