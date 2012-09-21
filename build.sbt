organization := "com.bostontechnologies"

name := "scala-amqp"

version := "0.3.0-SNAPSHOT"

resolvers +=  "Typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++=  Seq(
    "org.scalaz" %% "scalaz-core" % "6.0.4" ,
    "com.typesafe.akka" % "akka-actor" % "2.0.1",
    "com.rabbitmq" % "amqp-client" % "2.7.1",
    "com.weiglewilczek.slf4s" %% "slf4s" % "1.0.7")

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-Ydependent-method-types", "-unchecked")

