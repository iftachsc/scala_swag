scalaVersion := "2.13.8"

name := "hello-world"
organization := "ch.epfl.scala"
version := "1.0"

val zioVersion = "1.0.12"
val htt4sVersion = "0.21.22"
val circeVersion = "0.14.2"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1"
//libraryDependencies += "org.json4s"             %% "json4s-native"            % "4.0.5"

libraryDependencies ++= Seq(
    "dev.zio" %% "zio",
    "dev.zio" %% "zio-streams",
    
).map(_ % zioVersion)


libraryDependencies += "dev.zio" %% "zio-json" % "0.1.4"
libraryDependencies += "dev.zio" %% "zio-json-interop-http4s" % "0.1.5"

libraryDependencies ++= Seq(
    "org.http4s" %% "http4s-blaze-server",
    "org.http4s" %% "http4s-circe",
    "org.http4s" %% "http4s-dsl"
).map(_ % htt4sVersion)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-literal",
  "io.circe" %% "circe-generic-extras",
  "io.circe" %% "circe-parser"

 
).map(_ % circeVersion)

libraryDependencies += "dev.zio" %% "zio-interop-cats" % "3.2.9.1"

//libraryDependencies += "dev.zio" %% "zio-process" % "0.6.1"




