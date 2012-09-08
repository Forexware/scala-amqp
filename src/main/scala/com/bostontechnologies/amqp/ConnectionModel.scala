package com.bostontechnologies.amqp

import com.rabbitmq.client.{ShutdownListener, ConnectionFactory}
import akka.actor.{FSM, ActorRef}
import scalaz._
import scalaz.{State => Statez}
import Scalaz._
import ConsumerModel._
import ChannelModel._
import com.weiglewilczek.slf4s.Logging

object ConnectionModel extends Logging {

  type FSMStateType = FSM.State[ConnectionState, ConnectionData]

  case class ConnectionConfig(cf: ConnectionFactory, addresses: List[RabbitAddress])

  sealed trait connectionModel

  //State
  sealed trait ConnectionState

  case object Connecting extends ConnectionState

  case object Connected extends ConnectionState

  case object Disconnected extends ConnectionState

  //State Data
  case class ConnectionData(channels: Set[ActorRef], data: ConnectionStateData)

  sealed trait ConnectionStateData

  case object DisconnectedData extends ConnectionStateData

  case class ConnectingData(senders: List[ActorRef]) extends ConnectionStateData

  case class ConnectedData(c: RabbitConnection, s: ShutdownListener) extends ConnectionStateData


  val channelsLens: Lens[ConnectionData, Set[ActorRef]] = Lens(_.channels, (d, args) => d.copy(channels = args))
  val addChannel: ActorRef => State[ConnectionData, Set[ActorRef]] = ch => channelsLens.mods(_ + ch)
  val removeChannel: ActorRef => State[ConnectionData, Set[ActorRef]] = ch => channelsLens.mods(_ - ch)
  val dataLens: Lens[ConnectionData, ConnectionStateData] = Lens(_.data, (d, args) => d.copy(data = args))

  //Messages
  case object Disconnect

  case class Connect()

  case class ConnectResponse()

  case class NewConsumer(handleMsg: Either[ConsumerHandler, AckingConsumerHandler], handleError: ErrorHandler,
                         queues: Set[Queue], exchanges: Set[Exchange], bindings: Set[QueueBinding], actorName: String)


  case class NewConsumerResponse(consumer: Validation[Throwable, ActorRef @@ ConsumerRef])

  case class NewSender(exchange: Option[Exchange], handleError: ErrorHandler, actorName: String)

  case class NewSenderResponse(sender: Validation[Throwable, ActorRef @@ SenderRef])

  //  case class ProvideWithChannels(destination: ActorRef)
  case class StopProvideWithChannels(destination: ActorRef)

  case object RequestNewChannel

  case class NewChannel(ch: RabbitChannel)

  sealed trait SenderRef

  sealed trait ConsumerRef

  sealed trait ConnectionRef

  def addChannelToState[TRef](f: => Validation[Throwable, ActorRef @@ TRef])
  : Statez[ConnectionData, Validation[Throwable, ActorRef @@ TRef]] = {
    f.fold(
      e => state((_: ConnectionData) -> e.fail),
      channel => for {
        _ <- addChannel(channel)
      } yield channel.success
    )
  }

  def broadcastChannelTo[TRef](newConsumer: Validation[scala.Throwable, ActorRef @@ TRef])
                              (implicit createChannel: () => RabbitChannel) =
    state((_: ConnectionData) -> {
      for {
        ref <- newConsumer
        broadcast <- broadcastNewChannels(Set(ref))
      } yield ref
    })

  def broadcastNewChannels(dest: Set[ActorRef])
                          (implicit createChannel: () => RabbitChannel): Validation[Throwable, Unit] = {
    () => dest.foreach(dest => {
      logger.info("Broadcasting new channel to: " + dest)
      dest ! NewChannel(createChannel())
    })
  }.throws

}




















