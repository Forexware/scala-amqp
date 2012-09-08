package com.bostontechnologies

import scalaz._
import Scalaz._

package object amqp {
  /*
   * Copyright (c) 2011 Miles Sabin
   *
   * Licensed under the Apache License, Version 2.0 (the "License");
   * you may not use this file except in compliance with the License.
   * You may obtain a copy of the License at
   *
   *     http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */


  /**
   * This is used for annotating types.
   * http://etorreborre.blogspot.com/2011/11/practical-uses-for-unboxed-tagged-types.html
   *
   * This is excerpt from:
   * https://raw.github.com/milessabin/shapeless/master/src/main/scala/shapeless/typeoperators.scala
   */
  type Tagged[U] = {type Tag = U}
  type @@[T, U] = T with Tagged[U]

  class Tagger[U] {
    def apply[T](t: T): T @@ U = t.asInstanceOf[T @@ U]
  }

  def tag[U] = new Tagger[U]


  type =>?[-A, +B] = PartialFunction[A, B]

  type RabbitShutdownListener = com.rabbitmq.client.ShutdownListener
  type RabbitConnection = com.rabbitmq.client.Connection
  type RabbitChannel = com.rabbitmq.client.Channel
  type RabbitAddress = com.rabbitmq.client.Address
  type RabbitConsumer = com.rabbitmq.client.Consumer


  class PimpedValidation[+E, +A](val v: Validation[E, A]) {
    def failMap[EB](f: E => EB): Validation[EB, A] = f <-: v
  }

  class ValidationOfValidation[E, A](val v: Validation[E, Validation[E, A]]) {
    def flatten: Validation[E, A] = v.fold(e => e.fail)
  }

  class ListOfValidation[A, B](val seq: List[Validation[A, B]]) {


    def toValidationTakingFirstFailure: Validation[A, List[B]] = {
      import Validation.Monad._
      seq.sequence[({type l[a] = Validation[A, a]})#l, B]
    }

    def toValidationTakingAllFailures: Validation[List[A], List[B]] = {
      seq.traverse[({type l[a] = ValidationNEL[A, a]})#l, B](_.liftFailNel).failMap(_.list)
    }
  }


  def toValidationIfNotNull[B, A](a: => A, failure: => B): Validation[B, A] = {
    toValidation(Option(a), failure)
  }

  def toValidation[B, A](a: => Option[A], failure: => B): Validation[B, A] = {
    a.toSuccess(failure)
  }

  def throwFailure[E <: Throwable, A](a: => Validation[E, A]): A = a.fold(throw _, a1 => a1)

  implicit def toValidationOfValidation[E, A](v: Validation[E, Validation[E, A]]) = new ValidationOfValidation(v)

  implicit def toRichValidation[E, A](v: Validation[E, A]): PimpedValidation[E, A] = new PimpedValidation(v)

  implicit def seqOfValidationToValidationOfSeq[A, B](s: List[Validation[A, B]]): ListOfValidation[A, B] = new ListOfValidation(s)




}
