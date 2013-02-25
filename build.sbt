organization := "com.bostontechnologies"

name := "scala-amqp"

version := "0.4.0"

resolvers +=  "Typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++=  Seq(
    "org.scalaz" %% "scalaz-core" % "6.0.4" ,
    "com.typesafe.akka" % "akka-actor" % "2.0.1",
    "org.specs2" %% "specs2" % "1.13" % "test",
    "com.rabbitmq" % "amqp-client" % "2.7.1",
    "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
    "com.chuusai" %% "shapeless" % "1.2.3")

scalaVersion := "2.10.0"

scalacOptions ++= Seq("-deprecation", "-unchecked")

