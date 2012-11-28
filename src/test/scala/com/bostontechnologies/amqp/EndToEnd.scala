package com.bostontechnologies.amqp

import org.specs2.mutable.Specification
import akka.actor.{Props, ActorRef, ActorSystem}
import scalaz._
import Scalaz._
import shapeless.TypeOperators._
import com.bostontechnologies.amqp.ConnectionModel.{Disconnect, ConnectionConfig, ConnectionRef}
import com.bostontechnologies.amqp.ChannelModel.{QueueBinding, Queue, fanout, Exchange}
import com.bostontechnologies.amqp.ConsumerModel.AckableMessage
import com.rabbitmq.client.{ConnectionFactory, Address}
import Connection._
import util.Random
import com.bostontechnologies.amqp.ConnectionModel.ConnectionConfig
import com.bostontechnologies.amqp.ChannelModel.Exchange
import com.bostontechnologies.amqp.ChannelModel.QueueBinding
import com.bostontechnologies.amqp.ChannelModel.Queue
import java.util.concurrent.{CountDownLatch, TimeUnit}
import com.bostontechnologies.amqp.SenderModel.SendMessage

/**
 * This test follows the pattern of:
 *
 * 1) Create Connection Object
 * 2) Create Producer/Consumer given connection object
 * 3) Actually "Connect" using Connection Object (i.e. make TCP/IP connection etc.)
 */
object EndToEnd extends Specification{
  val addressList: List[Address] = List(new Address("localhost"))
  val exchangeName = "testExchange" + Random.nextInt(10000)
  val routingKey = "routingKey" + Random.nextInt(10000)
  val exchange = Exchange(exchangeName, fanout,  durable = true,  autoDelete = false)
  val queue = Queue("tesla.quotes.queue", exchange.durable,  exclusive = false,  autoDelete = exchange.autoDelete)

  "Amqp Sender And Receiver" should {
    "work" in {
      implicit val amqpActorSystem = ActorSystem("AmqpActorSystem")
      val rabbitConnectionFactory = new com.rabbitmq.client.ConnectionFactory()
      rabbitConnectionFactory.setRequestedHeartbeat(2)
      val countdownLatch = new CountDownLatch(1)
      //consumer
      (for {
        consumerConnection <- newConnection(ConnectionConfig(rabbitConnectionFactory, addressList), "TigerAmqpEntryPointConnection")
        _ <- Connection.autoAckingConsumerOf({(_,_) => countdownLatch.countDown() }, Set(queue), Set(exchange), Set(QueueBinding(queue.name, exchangeName, routingKey)), "Consumer")(consumerConnection)
        _ <- Connection.connect()(consumerConnection)
      } yield consumerConnection).fold(
        throwable => {
          throwable.printStackTrace()
          false
        }, consumerConnection => {
          (for {
            producerConnection <- newConnection(ConnectionConfig(new ConnectionFactory(), addressList), "s")
            producerSender <- senderOf(exchange.some, "s")(producerConnection)
            _ <- connect()(producerConnection)
          } yield (producerConnection, producerSender)).fold(
          throwable => {
            throwable.printStackTrace()
            false
          },{
            case (producerConnection, producerSender) => {
              producerSender ! SendMessage(routingKey, "hi".getBytes(), exchange.some)
              val result = countdownLatch.await(5, TimeUnit.SECONDS)
              producerConnection ! Disconnect
              consumerConnection ! Disconnect
              result
            }
          }
          )
        })
    }
  }

}
