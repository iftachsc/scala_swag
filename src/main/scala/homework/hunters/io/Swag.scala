import java.util.concurrent.ConcurrentLinkedDeque

sealed trait Swag {
    def combine = {
        ???
    }

    def lower = ???

    def lift = ???
}

case class SimpleSWAG[A,B](source: ConcurrentLinkedDeque[A], sink: ConcurrentLinkedDeque[B]) extends Swag

case class SmallSlideSWAG() extends Swag{

}
