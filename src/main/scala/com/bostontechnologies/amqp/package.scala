package com.bostontechnologies

import scalaz._
import Scalaz._

package object amqp {

  type =>?[-A, +B] = PartialFunction[A, B]

  type RabbitShutdownListener = com.rabbitmq.client.ShutdownListener
  type RabbitConnection = com.rabbitmq.client.Connection
  type RabbitChannel = com.rabbitmq.client.Channel
  type RabbitConsumer = com.rabbitmq.client.Consumer
  type RabbitAddress = com.rabbitmq.client.Address


  class PimpedValidation[+E, +A](val v: Validation[E, A]) {
    def failMap[EB](f: E => EB): Validation[EB, A] = f <-: v
  }

  class ValidationOfValidation[E, A](val v: Validation[E, Validation[E, A]]) {
    def flatten: Validation[E, A] = v.fold(e => e.fail)
  }

  class ListOfValidation[A, B](val seq: List[Validation[A, B]]) {

    def toValidationTakingAllFailures: Validation[List[A], List[B]] = {
      seq.traverse[({type l[a] = ValidationNEL[A, a]})#l, B](_.liftFailNel).failMap(_.list)
    }
  }

  implicit def toValidationOfValidation[E, A](v: Validation[E, Validation[E, A]]) = new ValidationOfValidation(v)

  implicit def toRichValidation[E, A](v: Validation[E, A]): PimpedValidation[E, A] = new PimpedValidation(v)

  implicit def seqOfValidationToValidationOfSeq[A, B](s: List[Validation[A, B]]): ListOfValidation[A, B] = new ListOfValidation(s)

}
