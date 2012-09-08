name := "scala-amqp"

resolvers +=  "Akka Maven2 repo" at "http://repo.akka.io/releases/"

resolvers +=  "Twitter repo" at "http://maven.twttr.com/"

resolvers +=  "Typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"

resolvers +=  "BT maven artifactory" at "http://maven.bostontechnologies.com/artifactory/libs-releases-local"

libraryDependencies +=  "org.scalaz" %% "scalaz-core" % "6.0.3"

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.1"

libraryDependencies += "com.rabbitmq" % "amqp-client" % "2.7.1"

libraryDependencies += "com.weiglewilczek.slf4s" %% "slf4s" % "1.0.7"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-Ydependent-method-types", "-unchecked")





