package com.scalableminds.kvservice

import com.scalableminds.kvservice.messages.messages.GetRequest

object Hi {
  def main(args: Array[String]) = {
    println("Hi!")
    val a = GetRequest("aCollection", "aKey", 5)
  }
}
