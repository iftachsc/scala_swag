scalaVersion := "2.13.8"

name := "hello-world"
organization := "ch.epfl.scala"
version := "1.0"

val zioVersion = "1.0.15"
val htt4sVersion = "0.23.12"

libraryDependencies ++= Seq(
    "dev.zio" %% "zio",
    "dev.zio" %% "zio-streams",
    
).map(_ % zioVersion)

libraryDependencies ++= Seq(
   "dev.zio" %% "zio-test",
   "dev.zio" %% "zio-test-sbt" 
).map(_ % zioVersion % "test")
testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

libraryDependencies += "dev.zio" %% "zio-json" % "0.1.4"

libraryDependencies ++= Seq(
    "org.http4s" %% "http4s-blaze-server",
    "org.http4s" %% "http4s-circe",
    "org.http4s" %% "http4s-dsl"
).map(_ % htt4sVersion)

libraryDependencies += "dev.zio" %% "zio-interop-cats" % "3.2.9.1"



