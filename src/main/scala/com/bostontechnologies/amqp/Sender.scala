package com.bostontechnologies.amqp

import com.weiglewilczek.slf4s.Logging
import scala.collection.JavaConverters._
import scalaz._
import scalaz.{State => Statez}
import Scalaz._
import ChannelModel._
import SenderModel._
import ConnectionModel._
import akka.actor.{ActorRef, FSM, Actor}

class Sender(exchangeOption: Option[Exchange], errorHandler: ErrorHandler, connectionRef: ActorRef)
  extends Actor with FSM[ChannelState, SenderData] with Logging {

  type FSMStateType = FSM.State[ChannelState, SenderData]

  startWith(Inactive, SenderData(none, exchangeOption, errorHandler, connectionRef))

  when(Inactive) {
    case Event(NewChannel(ch), data) => {
      val (newData, nextState) = attemptInitializeSender(ch)(data)
      goto(nextState) using newData
    }
  }

  when(Active) {
    case Event(NewChannel(ch), data) => {
      val (newData, nextState) = attemptInitializeSender(ch)(data)
      goto(nextState) using newData
    }
  }

  whenUnhandled {
    case Event(sendMsg@SendMessage(k, msg, exchange, props), data) => {
      for {
        ex <- (exchange orElse exchangeLens(data))
          .toSuccess(SendMessageException("no default exchange defined and none provided with send request", sendMsg))
        ch <- channelLens(data).toSuccess(SendMessageException("No channel", sendMsg))
        _ <- (() => ch.basicPublish(ex.name, k, props, msg)).throws
      } yield {}
    }.fold(
      e => {
        logger.error("Error encountered while sending: " + e)
        handleErrorLens(data)(e)
        goto(Inactive) using data
      },
      _ => stay using data)
  }

  initialize

  private def attemptInitializeSender(ch: RabbitChannel): Statez[SenderData, ChannelState] = for {
    _ <- channelLens := ch.some
    declareResult <- declareExchange
    nextState <- state[SenderData, ChannelState](d => {
      declareResult.fold(
        e => {
          logger.error("Unable to declare items: " + e)
          (d, Inactive)
        },
        _ => {
          logger.info("Sender has been initialized: " + d)
          (d, Active)
        })
    })
  } yield nextState

  private def declareExchange: Statez[SenderData, Validation[List[Throwable], List[OpStatus[_]]]] =
    state(data => (data, {
      val result = for {
        ch <- channelLens(data).toList
        Exchange(name, exType, durable, autoDelete, args) <- exchangeLens(data)
      } yield {
          () => OpStatus(ch.exchangeDeclare(name, exType.toString, durable, autoDelete, args.asJava))
        }.throws
      result.toValidationTakingAllFailures
    }))
}
