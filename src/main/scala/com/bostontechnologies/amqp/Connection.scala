package com.bostontechnologies.amqp

import com.weiglewilczek.slf4s.Logging

import akka.pattern.ask
import scalaz._
import Scalaz._
import ConnectionModel._
import akka.util.duration._
import akka.dispatch.Await
import akka.util.{Duration, Timeout}
import ConsumerModel._
import akka.actor._
import com.rabbitmq.client.ShutdownSignalException

/**
 * Represents the state of a Connection
 */
class Connection(config: ConnectionConfig) extends Actor with FSM[ConnectionState, ConnectionData] with Logging {

  private def newSenderActor(req: NewSender) = {
    () => tag[SenderRef](context.actorOf(Props(new Sender(req.exchange, req.handleError, context.self)), req.actorName))
  }.throws

  private def newConsumerActor(req: NewConsumer) = {
    () => tag[ConsumerRef](context.actorOf(Props(
      new Consumer(
        req.handleMsg,
        req.handleError,
        req.queues,
        req.exchanges,
        req.bindings,
        context.self)),
      req.actorName))
  }.throws

  val connectTimeout = 2.5 seconds
  val abortTimeout = 10 seconds

  startWith(Disconnected, ConnectionData(channels = Set(), data = DisconnectedData))

  when(Disconnected) {
    case Event(Connect(), data) => {
      logger.info("Starting connection attempts")
      attemptConnect(channelsLens ! data, ConnectingData(List(context.sender)))
    }
  }

  when(Connecting, connectTimeout) {
    case Event(StateTimeout, data@ConnectionData(_, cd: ConnectingData)) => {
      logger.info("Retrying Connection")
      attemptConnect(channelsLens(data), cd)
    }
  }

  when(Connected) {
    case Event(req: NewSender, data@ConnectionData(ch, ConnectedData(c, _))) => {
      val createChannelAndBroadcast = for {
        newSender <- addChannelToState[SenderRef](newSenderActor(req))
        broadcastResult <- broadcastChannelTo[SenderRef](newSender)(c.createChannel)
      } yield broadcastResult

      val (newData, newSenderResult) = createChannelAndBroadcast(data)
      stay using newData replying newSenderResult.fold(
        e => NewSenderResponse(e.fail),
        sender => NewSenderResponse(sender.success)
      )
    }

    case Event(req: NewConsumer, data@ConnectionData(ch, ConnectedData(c, _))) => {
      val createChannelAndBroadcast = for {
        newConsumer <- addChannelToState[ConsumerRef](newConsumerActor(req))
        broadcastResult <- broadcastChannelTo[ConsumerRef](newConsumer)(c.createChannel)
      } yield broadcastResult

      val (newData, newConsumerResult) = createChannelAndBroadcast(data)
      stay using newData replying newConsumerResult.fold(
        e => NewConsumerResponse(e.fail),
        ref => NewConsumerResponse(ref.success)
      )
    }

    case Event(RequestNewChannel, data@ConnectionData(ch, ConnectedData(c, _))) => {
      broadcastNewChannels(Set(context.sender))(c.createChannel)
      stay
    }

    case Event(Disconnect, data@ConnectionData(_, ConnectedData(c, sl))) => {
      {
        () => {
          logger.info("Disconnecting")
          c.removeShutdownListener(sl)
          disconnect(c)
        }
      }.throws.fold(e => logger.error("Error disconnecting", e))
      goto(Disconnected) using (dataLens.set(data, DisconnectedData))
    }
    case Event(InternalDisconnected(e), data@ConnectionData(ch, ConnectedData(c, sl))) => {
      logger.info("Disconnection Event Received while Connected: " + e)
      c.removeShutdownListener(sl)
      attemptConnect(ch, ConnectingData(List()))
    }
  }

  whenUnhandled {
    case Event(req: NewSender, data) => {
      val (newData, ch) = (for {
        ch <- addChannelToState[SenderRef](newSenderActor(req))
      } yield ch)(data)
      stay using newData replying NewSenderResponse(ch)
    }

    case Event(req: NewConsumer, data) => {
      val (newData, ch) = (for {
        ch <- addChannelToState[ConsumerRef](newConsumerActor(req))
      } yield ch)(data)
      stay using newData replying NewConsumerResponse(ch)
    }

    case Event(StopProvideWithChannels(s), data) => stay() using removeChannel(s) ~> data
    case Event(e, s) => {
      logger.warn("Unhandled event: " + e + " for state: " + s)
      stay
    }
  }

  initialize

  private case class InternalDisconnected(e: ShutdownSignalException)

  private def disconnect: RabbitConnection => Unit = ch => ch.abort(abortTimeout.toMillis.asInstanceOf[Int])

  private def attemptConnect(channels: Set[ActorRef], data: ConnectingData): FSMStateType = {
    for {
      c <- connect(config)
      _ <- broadcastNewChannels(channels)(c.createChannel)
    } yield c
  }.fold(e => {
    logger.error("Failed to connect: " + e)
    goto(Connecting) using ConnectionData(channels, data)
  }, c => {
    logger.info("Connected")
    val sl = new ShutdownListener(self)
    c.addShutdownListener(sl)
    data.senders.foreach(_ ! ConnectResponse())
    goto(Connected) using ConnectionData(channels, ConnectedData(c, sl))
  })

  private def connect: ConnectionConfig => Validation[Throwable, RabbitConnection] = config => {
    logger.info("Connecting")
    () => config.cf.newConnection(config.addresses.toArray)
  }.throws

  private class ShutdownListener(c: ActorRef) extends RabbitShutdownListener {
    def shutdownCompleted(e: ShutdownSignalException) {
      c ! InternalDisconnected(e)
    }
  }

}

object Connection extends Logging {

  implicit val awaitDuration: Duration = 10 seconds
  implicit val awaitTimeout: Timeout = awaitDuration

  import ChannelModel._

  def newConnection(config: ConnectionConfig, actorName: String)(implicit system: ActorSystem) = {
    () => tag[ConnectionRef](system.actorOf(Props(new Connection(config)), name = actorName))
  }.throws

  def senderOf(ex: Option[Exchange], actorName: String, errorHandler: ErrorHandler = loggingSenderErrorHandler)
              (implicit connection: ActorRef @@ ConnectionRef) = {
    () => Await.result(connection ? NewSender(ex, errorHandler, actorName), awaitDuration)
      .asInstanceOf[NewSenderResponse].sender
  }.throws.flatten

  def autoAckingConsumerOf(deliveryHandler: ConsumerHandler, queues: Set[Queue], exchanges: Set[Exchange],
                 bindings: Set[QueueBinding], actorName: String,
                 handleErrors: ErrorHandler = loggingConsumerErrorHandler)
                (implicit connection: ActorRef @@ ConnectionRef) = {
    () => Await.result(
      connection ? NewConsumer(Left(deliveryHandler), handleErrors, queues, exchanges, bindings, actorName),
      awaitDuration)
      .asInstanceOf[NewConsumerResponse].consumer
  }.throws.flatten

  def manualAckingConsumerOf(ackingDeliveryHandler: AckingConsumerHandler, queues: Set[Queue], exchanges: Set[Exchange],
                           bindings: Set[QueueBinding], actorName: String,
                           handleErrors: ErrorHandler = loggingConsumerErrorHandler)
                          (implicit connection: ActorRef @@ ConnectionRef) = {
    () => Await.result(
      connection ? NewConsumer(Right(ackingDeliveryHandler), handleErrors, queues, exchanges, bindings, actorName),
      awaitDuration)
      .asInstanceOf[NewConsumerResponse].consumer
  }.throws.flatten

  def connect()(implicit connection: ActorRef @@ ConnectionRef) = {
    () => {
      Await.result(connection ? Connect(), awaitDuration).asInstanceOf[ConnectResponse]
      connection
    }
  }.throws

  val loggingConsumerErrorHandler = (e: Throwable) => logger.error("Error encountered in RabbitMQ Consumer", e)
  val loggingSenderErrorHandler = (e: Throwable) => logger.error("Error encountered in RabbitMQ Sender", e)


}