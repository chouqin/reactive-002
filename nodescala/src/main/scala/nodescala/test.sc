//package nodescala

import nodescala._

import scala.concurrent.Future
import scala.concurrent.duration._

val working = Future.run() { ct =>
  Future {
    while (ct.nonCancelled) {
      println("working")
    }
    println("done")
  }
}
Future.delay(5 seconds) onSuccess {
  case _ => working.unsubscribe()
}
