package com.bostontechnologies.amqp

import com.rabbitmq.client.AMQP.BasicProperties
import akka.actor.ActorRef
import scalaz._
import Scalaz._
import ChannelModel._

object SenderModel {

  //Messages
  case class SendMessage(key: String, msg: Array[Byte], exchange: Option[Exchange] = none, properties: BasicProperties = SendMessage.defaultProperties)

  object SendMessage {
    val defaultProperties = new BasicProperties.Builder().build()
  }

  case class SendMessageException(description: String, msg: SendMessage) extends Exception(description)

  //Sender Data
  case class SenderData(channel: Option[RabbitChannel], exchanges: Option[Exchange], handleErrors: ErrorHandler, connection: ActorRef)

  val channelLens: Lens[SenderData, Option[RabbitChannel]] = Lens(_.channel, (data, arg) => data.copy(channel = arg))
  val exchangeLens: Lens[SenderData, Option[Exchange]] = Lens(_.exchanges, (data, arg) => data.copy(exchanges = arg))
  val handleErrorLens: Lens[SenderData, Throwable => Unit] = Lens(_.handleErrors, (data, arg) => data.copy(handleErrors = arg))
  val connectionLens: Lens[SenderData, ActorRef] = Lens(_.connection, (data, arg) => data.copy(connection = arg))

}
