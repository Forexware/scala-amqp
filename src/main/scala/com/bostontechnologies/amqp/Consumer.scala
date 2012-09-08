package com.bostontechnologies.amqp

import com.weiglewilczek.slf4s.Logging
import com.rabbitmq.client.AMQP.BasicProperties
import akka.pattern.ask
import scalaz._
import Scalaz._
import ChannelModel._
import ConsumerModel._
import ConnectionModel._
import akka.util.duration._
import akka.dispatch.Await
import akka.util.{Timeout, Duration}
import akka.actor._
import com.rabbitmq.client.{Envelope, ShutdownSignalException}
import akka.actor.FSM.Normal

class Consumer(val handleDelivery: Either[ConsumerHandler, AckingConsumerHandler],
               val handleErrors: ErrorHandler,
               queues: Set[Queue], exchanges: Set[Exchange], bindings: Set[QueueBinding], connection: ActorRef)
  extends Actor with FSM[ConsumerState, ConsumerData] with Logging {

  startWith(WaitingForNewChannel, ConsumerData(none, newConsumer, handleDelivery.isLeft, exchanges, queues, bindings))

  private def newConsumer = (q: Queue) => {
    new QueueConsumer(self, q)
  }

  when(WaitingForNewChannel) {
    case Event(shutdown: ShutdownSignal, data) => stay //ignore these
    case Event(NewChannel(ch), data) => {
      //Only want to use New Channel if we are waiting for it
      val (newData, result) = newConsumersForNewChannel(ch)(data)
      result.failMap(AggregateThrowable).fold(e => {
        logger.error("Error consuming", e)
        handleErrors(e)
      })
      goto(ChannelActive) using newData
    }
  }

  when(ChannelActive) {
    case Event(CancelledByBroker(tag, q), data) => {
      val (newData, result) = newConsumerForQIfExists(Set(q))(data)
      result.failMap(AggregateThrowable).fold(e => {
        logger.error("Error attempting to reconsume after it was cancelled", e)
        handleErrors(e)
      })
      stay using newData
    }

    case Event(ShutdownSignal(tag, ex, q), data) => {
      //only want to receive this once
      goto(WaitingForNewChannel) using {
        for {
          _ <- channelLens := none
          _ <- state[ConsumerData, Unit](_ -> (connection ! RequestNewChannel))
        } yield {}
      } ~> data
    }

    case Event(DeclareAndConsumeQueue(q, b, ex), data) => {
      val (newData, result) = (for {
        queues <- addQueue(q)
        bindings <- optionallyAddBinding(b)
        exchanges <- optionallyAddExchange(ex)
        results <- newConsumerForQIfExists(Set(q))
      } yield results.failMap(AggregateThrowable).map(_ => q))(data)
      stay using newData replying DeclareAndConsumeQueueResponse(result) //respond to request
    }

    case Event(req@RemoveConsumerFromQueue(q), data) => {
      logger.info("Removing Consumer from Queue: " + req)
      val (newData, result) = (for {
        queues <- queuesLens
        results <- removeQueuesAndCancelConsumers(queues.filter(_.name === q))
      } yield results.failMap(AggregateThrowable).map(_ => q))(data)
      stay using newData replying RemoveConsumerFromQueueResponse(result) //respond to request
    }

    case Event(req@RemoveQueueBinding(qName, exName, key), data) => {
      logger.info("Removing binding: " + req)
      val (newData, result) = (removeBindingFromChannel(qName, exName, key))(data)
      stay using newData replying RemoveQueueBindingResponse(result.map(_ => {}))
    }

    case Event(StopConsumer, data) => {
      logger.info("Stopping consumer with data: " + data)
      connection ! StopProvideWithChannels(context.self)
      stop(Normal) using removeAndCancelChannel ~> data
    }

    case Event(NewChannel(ch), data) => {
      logger.info("Received new channel when not expected, closing it. This could happen when connection unexpectedly broker")
      logThrowables(ch.close())
      stay
    }
  }

  whenUnhandled {

    case Event(Delivery(_, header, _, msg), _) => {
      handleDelivery.fold(handle => handle(msg, header.getDeliveryTag),
        ackingHandle => ackingHandle(AckableMessage(msg, header.getDeliveryTag, self)))
      stay
    }

    case Event(Acknowledge(tag), data) => {
      data.channel.foreach(_.basicAck(tag, false))
      stay
    }

    case Event(Reject(tag, requeue), data) => {
      data.channel.foreach(_.basicReject(tag, requeue))
      stay
    }
    case Event(CancelledByChannel(tag, q), data) => stay
    case Event(ConsumeOk(tag, q), data) => stay //Eventually will need to stop timer here
    case Event(StopConsumer, data) => {
      connection ! StopProvideWithChannels(context.self)
      stop(Normal) using (channelLens := none) ~> data
    }
    case Event(e, data) => {
      logger.info("Unhandled message: " + e + " in state: " + stateName + " and data: " + data)
      stay
    }
  }

  private def logThrowables[T](f: => T): Validation[Throwable, T] = {
    () => f
  }.throws.failMap(e => {
    logger.error("Error handled:" + e)
    e
  })

  //QueueConsumer Messages
  private case class Delivery(tag: String, header: Envelope, properties: BasicProperties, msg: Array[Byte])

  private case class CancelledByBroker(tag: String, q: Queue)

  private case class CancelledByChannel(tag: String, q: Queue)

  private case class ConsumeOk(tag: String, q: Queue)

  private case class ShutdownSignal(tag: String, e: ShutdownSignalException, q: Queue)

  /**
   * An instance of this is to be passed to the RabbitMQ infrastructure when consuming
   */
  private class QueueConsumer(handleDelivery: ActorRef, q: Queue) extends RabbitConsumer with Logging {
    def handleConsumeOk(tag: String) {
      logger.info("HandleConsumeOk - tag:" + tag)
      handleDelivery ! ConsumeOk(tag, q)
    }

    def handleCancelOk(tag: String) {
      logger.info("HandleCancelOk - tag:" + tag)
      handleDelivery ! CancelledByChannel(tag, q)
    }

    def handleCancel(tag: String) {
      logger.info("HandleCancel - tag:" + tag)
      handleDelivery ! CancelledByBroker(tag, q)
    }

    def handleShutdownSignal(tag: String, e: ShutdownSignalException) {
      logger.info("HandleShutdownSignal - tag:" + tag + " " + e +
        " isInitiatedByApplication: " + e.isInitiatedByApplication +
        " Reason: " + e.getReason +
        " Reference: " + e.getReference
      )
      handleDelivery ! ShutdownSignal(tag, e, q)
    }

    def handleRecoverOk(tag: String) {
      logger.info("HandleRecoverOk - tag:" + tag)
    }

    def handleDelivery(tag: String, header: Envelope, properties: BasicProperties, msg: Array[Byte]) {
      logger.info("Msg received - tag:" + tag)
      handleDelivery ! Delivery(tag, header, properties, msg)
    }
  }

  def useUnhandledEventHandler = new (Event =>? State) {
    def apply(v1: Event): State = stay

    def isDefinedAt(x: Event): Boolean = false
  }

}

object Consumer {

  implicit val awaitDuration: Duration = 10 seconds
  implicit val awaitTimeout: Timeout = awaitDuration

  def declareAndConsumeQueue(queue: Queue, bindingOption: Option[QueueBinding] = none, exchangeOption: Option[Exchange] = none)
                            (implicit consumer: ActorRef @@ ConsumerRef): Validation[Throwable, Queue] = {
    () => Await.result(consumer ? DeclareAndConsumeQueue(queue, bindingOption, exchangeOption), awaitDuration)
      .asInstanceOf[DeclareAndConsumeQueueResponse].value
  }.throws.flatten

  def removeConsumerFromQueue(queueName: String)
                             (implicit consumer: ActorRef @@ ConsumerRef): Validation[Throwable, String] = {
    () => Await.result(consumer ? RemoveConsumerFromQueue(queueName), awaitDuration)
      .asInstanceOf[RemoveConsumerFromQueueResponse].value
  }.throws.flatten

  def removeQueueBinding(queueName: String, exchangeName: String, key: String)
                        (implicit consumer: ActorRef @@ ConsumerRef): Validation[Throwable, Unit] = {
    () => Await.result(consumer ? RemoveQueueBinding(queueName, exchangeName, key), awaitDuration)
      .asInstanceOf[RemoveQueueBindingResponse].value
  }.throws.flatten


  def stopConsumer(implicit consumer: ActorRef @@ ConsumerRef) {
    consumer ! StopConsumer
  }

}
