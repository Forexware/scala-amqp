package com.bostontechnologies.amqp

import com.weiglewilczek.slf4s.Logging
import scala.collection.JavaConverters._
import ChannelModel._
import scalaz._
import Scalaz._
import akka.actor.ActorRef

object ConsumerModel extends Logging {
  type DeliveryTag = Long
  type ConsumerHandler = (Array[Byte], DeliveryTag) => Unit
  type AckingConsumerHandler = (AckableMessage) => Unit
  type QueueConsumerRef = (String, RabbitConsumer)

  case class AckableMessage(payload: Array[Byte], tag: DeliveryTag, sender: ActorRef) {
    def acknowledge() {
      sender ! Acknowledge(tag)
    }

    def reject(requeue: Boolean = false) {
      sender ! Reject(tag, requeue)
    }
  }

  case class Acknowledge(tag: DeliveryTag)

  case class Reject(tag: DeliveryTag, requeue: Boolean)

  case class AggregateThrowable(exceptions: List[Throwable]) extends Throwable

  sealed trait ConsumerState

  case object WaitingForNewChannel extends ConsumerState

  case object ChannelActive extends ConsumerState

  //Messages
  case object StopConsumer

  case class DeclareAndConsumeQueue(queue: Queue, bindingOption: Option[QueueBinding] = none, exchangeOption: Option[Exchange] = none)

  case class DeclareAndConsumeQueueResponse(value: Validation[Throwable, Queue])

  case class DeclareExchange(exchange: Exchange)

  case class DeclareBinding(binding: QueueBinding)

  case class RemoveConsumerFromQueue(name: String)

  case class RemoveConsumerFromQueueResponse(value: Validation[Throwable, String])

  case class RemoveQueueBinding(queueName: String, exchangeName: String, key: String)

  case class RemoveQueueBindingResponse(value: Validation[Throwable, Unit])

  case class DeleteExchange(name: String)


  //Consumer data
  case class ConsumerData(channel: Option[RabbitChannel],
                          newConsumer: Queue => RabbitConsumer,
                          autoAck: Boolean,
                          exchanges: Set[Exchange] = Set(),
                          queues: Set[Queue] = Set(),
                          bindings: Set[QueueBinding] = Set(),
                          activeConsumers: Map[Queue, QueueConsumerRef] = Map())

  val channelLens: Lens[ConsumerData, Option[RabbitChannel]] =
    Lens(_.channel, (d, arg) => d.copy(channel = arg))

  val queuesLens: Lens[ConsumerData, Set[Queue]] =
    Lens(_.queues, (d, arg) => d.copy(queues = arg))

  def addQueue(arg: Queue) = queuesLens.mods(_ + arg)

  val exchangesLens: Lens[ConsumerData, Set[Exchange]] =
    Lens(_.exchanges, (d, arg) => d.copy(exchanges = arg))

  def optionallyAddExchange(arg: Option[Exchange]) = exchangesLens.mods(exs => arg.some(exs + _).none(exs))

  val bindingsLens: Lens[ConsumerData, Set[QueueBinding]] =
    Lens(_.bindings, (d, arg) => d.copy(bindings = arg))

  def removeBindingsForQueues(arg: Set[Queue]) =
    bindingsLens.mods(bs => bs.filterNot(b => arg.any(_.name === b.destination)))

  def optionallyAddBinding(arg: Option[QueueBinding]) = bindingsLens.mods(bs => arg.some(bs + _).none(bs))

  val consumerLens: Lens[ConsumerData, Map[Queue, QueueConsumerRef]] =
    Lens(_.activeConsumers, (d, arg) => d.copy(activeConsumers = arg))

  val addConsumers: List[(Queue, QueueConsumerRef)] => State[ConsumerData, Map[Queue, QueueConsumerRef]] =
    c => consumerLens.mods(m => m ++ c)

  val addConsumer: ((Queue, QueueConsumerRef)) => State[ConsumerData, Map[Queue, QueueConsumerRef]] =
    c => consumerLens.mods(m => m + c)

  def initialize(initialize: State[ConsumerData, Validation[List[Throwable], Map[Queue, QueueConsumerRef]]])
                (data: ConsumerData)
  : Validation[List[Throwable], ConsumerData] = {
    val (newData, results) = initialize(data)
    results map (_ => newData)
  }

  def newConsumersForNewChannel = (ch: RabbitChannel) => for {
    _ <- channelLens := ch.some
    qs <- queuesLens
    dependencies <- findDependencies(qs)
    consumers <- declareItemsAndConsume(dependencies)
  } yield {
    consumers
  }

  def newConsumerForQIfExists = (qs: Set[Queue]) => for {
    dependencies <- findDependencies(qs)
    consumers <- declareItemsAndConsume(dependencies)
  } yield consumers

  def findDependencies(queues: Set[Queue]): State[ConsumerData, (Set[Queue], Set[QueueBinding], Set[Exchange])] =
    state(data => (data, {
      val qs = queuesLens(data) & queues
      val qBs = bindingsLens(data).filter(b => qs.any(_.name === b.destination))
      val ex = exchangesLens(data).filter(ex => qBs.any(_.exchange === ex.name))
      (qs, qBs, ex)
    }))

  def declareItemsAndConsume: ((Set[Queue], Set[QueueBinding], Set[Exchange])) =>
    State[ConsumerData, Validation[List[Throwable], Map[Queue, QueueConsumerRef]]] = {
    case (queues, qBindings, exchanges) =>
      state(data => {
        def declareAndConsume = for {
          ch <- channelLens(data).toSuccess(List(new ChannelException("Channel does not exist")))
          qs <- declareQueues(ch, queues)
          exs <- declareExchanges(ch, exchanges)
          qbs <- declareQueueBindings(ch, qBindings)
          existingConsumers <- reconsume(ch, consumerLens(data).filterKeys(queues.contains), data.autoAck)
          newQueues <- (queues &~ existingConsumers.keySet).success
          consumers <- consume(ch, newQueues, data.newConsumer, data.autoAck)
        } yield consumers ++ existingConsumers
        declareAndConsume.fold(
          errors => (data, errors.fail),
          c => consumerLens.modps(prev => ((prev ++ c), c.success))(data))
      })
  }

  val removeQueuesAndCancelAllConsumers: State[ConsumerData, Validation[List[Throwable], List[Queue]]] = for {
    allQueues <- queuesLens
    results <- removeQueuesAndCancelConsumers(allQueues)
  } yield results

  val removeAndCancelChannel: State[ConsumerData, Validation[Throwable, Unit]] = for {
    ch <- channelLens
    _ <- channelLens := none
  } yield {
      () => ch.foreach(_.close())
    }.throws

  def removeQueuesAndCancelConsumers(queues: Set[Queue]): State[ConsumerData, Validation[List[Throwable], List[Queue]]] = for {
    _ <- queuesLens.mods(qs => qs &~ queues) //remove queues first in case connection is broken
    _ <- removeBindingsForQueues(queues)
    allConsumers <- consumerLens
    consumersToRemove = allConsumers.filterKeys(queues.contains).toSet
    cancelResults <- cancelAndRemoveConsumers(consumersToRemove)
  } yield cancelResults

  def cancelAndRemoveConsumers(consumers: Set[(Queue, QueueConsumerRef)])
  : State[ConsumerData, Validation[List[Throwable], List[Queue]]] = {
    type ConsumersDataState[x] = State[ConsumerData, x]
    for {
      results <- consumers.toList.traverse[ConsumersDataState, Validation[Throwable, Queue]](cancelAndRemoveConsumer)
    } yield results.toValidationTakingAllFailures
  }

  def cancelAndRemoveConsumer(consumer: (Queue, QueueConsumerRef)): State[ConsumerData, Validation[Throwable, Queue]] =
    state(data => {
      for {
        ch <- channelLens(data).toSuccess(new ChannelException("Channel does not exist"))
        q <- cancelConsumer(ch, consumer)
      } yield q
    }.fold(
      e => (data, e.fail),
      q => (consumerLens.mod(data, consumers => consumers - q), q.success)))

  def cancelConsumer(channel: RabbitChannel, consumer: (Queue, QueueConsumerRef)): Validation[Throwable, Queue] = {
    val (q, (tag, _)) = consumer
    () => {
      channel.basicCancel(tag)
      q
    }
  }.throws

  def reconsume(channel: RabbitChannel, consumers: Map[Queue, QueueConsumerRef], autoAck: Boolean)
  : Validation[List[Throwable], Map[Queue, QueueConsumerRef]] = {
    for {
      (q, (tag, consumer)) <- consumers.toList
    } yield {
      logger.info("attempting to re-declare consumer for q: " + q + " on channel " + channel)
      () => (q ->(channel.basicConsume(q.name, autoAck, tag, consumer), consumer))
    }.throws
  }.toValidationTakingAllFailures.map(_.toMap)

  def consume(channel: RabbitChannel, queues: Set[Queue], newConsumer: Queue => RabbitConsumer,
              autoAck: Boolean)
  : Validation[List[Throwable], Map[Queue, QueueConsumerRef]] = {
    for {
      q <- queues.toList
    } yield {
      logger.info("attempting to declare new consumer for q: " + q + " on channel " + channel)
      val consumer = newConsumer(q)
      () => q ->(channel.basicConsume(q.name, autoAck, consumer), consumer)
    }.throws
  }.toValidationTakingAllFailures.map(_.toMap)

  def declareQueueBindings(channel: RabbitChannel, queueBindings: Set[QueueBinding]): Validation[List[Throwable], List[OpStatus[_]]] = {
    for {
      b@QueueBinding(queue, exchange, routingKey) <- queueBindings
    } yield {
      logger.info("declaring queue binding: " + b)
      () => OpStatus(channel.queueBind(queue, exchange, routingKey))
    }.throws
  }.toList.toValidationTakingAllFailures

  def declareQueues(ch: RabbitChannel, queues: Set[Queue]): Validation[List[Throwable], List[OpStatus[_]]] = {
    for {
      q <- queues.toList
    } yield {
      logger.info("declaring Queue: " + q)
      () => OpStatus(ch.queueDeclare(q.name, q.durable, q.exclusive, q.autoDelete, q.arguments.asJava))
    }.throws
  }.toList.toValidationTakingAllFailures

  def declareExchanges(ch: RabbitChannel, exchanges: Set[Exchange]): Validation[List[Throwable], List[OpStatus[_]]] = {
    for {
      ex@Exchange(name, exType, durable, autoDelete, args) <- exchanges
    } yield {
      logger.info("declaring exchange: " + ex)
      () => OpStatus(ch.exchangeDeclare(name, exType.toString, durable, autoDelete, args.asJava))
    }.throws
  }.toList.toValidationTakingAllFailures

  def removeBindingFromChannel(qName: String, exName: String, key: String)
  : State[ConsumerData, Validation[Throwable, Unit]] =
    for {
      channelOption <- channelLens
      _ <- bindingsLens.mods(_.filter {
        b => (b.destination === qName) && (b.exchange === exName) && (b.routingKey === key)
      })
    } yield {
      //Need to still remove the original binding
      () =>
        channelOption
          .toSuccess(ChannelException("Unable to remove binding because channel does not exist"))
          .foreach(ch => ch.queueUnbind(qName, exName, key))
    }.throws
}
