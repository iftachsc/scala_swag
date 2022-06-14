package ai.hunters.homework.iftach.zio.challange

import zio._
import zio.console._
import zio.interop.catz._
import zio.interop.catz.implicits._
import cats.implicits._
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s._
import org.http4s.implicits._
import zio.stream._
import zio.duration._
import org.http4s.circe._
import io.circe._
import zio.json._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s._
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._

import zio.interop.catz.implicits._

import scala.concurrent.ExecutionContext.global

object Http4sServer {

    private def swagApp(state: Ref[Map[(String,String),Int]]) = {
        val dsl = Http4sDsl[Task]
        import dsl._
        HttpRoutes.of[Task] {
           case GET -> Root / "window" => 
                state.get.flatMap(window => Ok(window.toString))

           case GET -> Root / "type" / eventType =>
                state.get.flatMap(window => Ok(window.filter(x => x._1._1 == eventType).map(x => x._1._2 -> x._2).toMap))
    }.orNotFound}

    def server(state: Ref[Map[(String,String),Int]]) = {
        
        for {
        server <- ZIO.runtime[ZEnv]
        .flatMap { implicit runtime =>
            BlazeServerBuilder[Task](runtime.platform.executor.asEC)
            .bindHttp(8080, "localhost")
            .withHttpApp(swagApp(state))
            .resource
            .toManagedZIO
            .useForever
            .foldCauseM(
                err => putStrLn(err.prettyPrint).as(ExitCode.failure),
                _ => ZIO.succeed(ExitCode.success)
            )
        }
    } yield server
}
    
}

