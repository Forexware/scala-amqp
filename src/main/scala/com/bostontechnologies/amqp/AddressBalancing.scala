package com.bostontechnologies.amqp

import util.Random
import com.rabbitmq.client.Address

sealed trait PreferredHost

object AddressBalancing {

  lazy val localHost = tag[PreferredHost]("127.0.0.1")

  def randomize(addresses: Array[Address], currentHost: String @@ PreferredHost = localHost): List[Address] = {
    val (localhost, remotes) = addresses.partition(a => a.getHost.startsWith(currentHost))
    localhost.toList ::: Random.shuffle(remotes.toList)
  }

  def parseAddresses(addresses: String): Array[Address] = {
    Address.parseAddresses(addresses)
  }

}