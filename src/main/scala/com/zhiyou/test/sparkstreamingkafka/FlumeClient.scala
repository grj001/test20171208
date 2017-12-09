package com.zhiyou.test.sparkstreamingkafka

import java.nio.charset.Charset

import org.apache.flume.api.RpcClientFactory
import org.apache.flume.event.EventBuilder

class FlumeClient(hostname: String, port: Int) {
  val client = RpcClientFactory.getDefaultInstance(hostname, port)

  //.getDefaultInstance(hostname.port)
  def sendString(msg: String) = {
    val event = EventBuilder.withBody(msg, Charset.forName("UTF-8"))
    client.append(event)
    println(s"sended:$msg")

  }

  def close() = {
    client.close()
  }
}

object FlumeClient {
  def apply(hostname: String, port: Int): FlumeClient =
    new FlumeClient(hostname, port)
}
