package com.bostontechnologies.amqp

import scalaz._
import Scalaz._

object ChannelModel {

  type ErrorHandler = Throwable => Unit

  //States
  sealed trait ChannelState

  case object Active extends ChannelState

  case object Inactive extends ChannelState

  case class OpStatus[T](status: T)

  //objects
  case class Queue(name: String,
                   durable: Boolean = true,
                   exclusive: Boolean = false,
                   autoDelete: Boolean = false,
                   arguments: Map[String, AnyRef] = Map())

  implicit def qEqual: Equal[Queue] = equalA

  case class ChannelException(description: String) extends Exception(description)

  case class QueueBinding(destination: String, exchange: String, routingKey: String)

  implicit def qBindingEqual: Equal[QueueBinding] = equalA

  case class Exchange(name: String,
                      exchangeType: ExchangeType,
                      durable: Boolean = true,
                      autoDelete: Boolean = false,
                      arguments: Map[String, AnyRef] = Map())

  sealed trait ExchangeType

  case object direct extends ExchangeType

  case object topic extends ExchangeType

  case object fanout extends ExchangeType

  case object headers extends ExchangeType

  case object system extends ExchangeType

  implicit def ExchangeEquals: Equal[ExchangeType] = equalA


}
