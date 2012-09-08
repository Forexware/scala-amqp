package com.bostontechnologies.amqp

import ChannelModel.{QueueBinding, Queue, Exchange}

trait BindingDSL {

  def from(exchange: Exchange): To = {
    new To {
      def to(q: Queue): this.type#UsingKey = new this.type#UsingKey {
        def usingKey(key: String): QueueBinding = QueueBinding(q.name, exchange.name, key)
      }
    }
  }

  trait To {

    def to(q: Queue): UsingKey

    trait UsingKey {
      def usingKey(key: String): QueueBinding
    }

  }

}


object BindingDSL extends BindingDSL