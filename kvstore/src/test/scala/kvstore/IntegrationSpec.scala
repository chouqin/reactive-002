/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.actor.{ Actor, Props, ActorRef, ActorSystem }
import akka.testkit.{ TestProbe, ImplicitSender, TestKit }
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import scala.concurrent.duration._
import scala.util.Random
import org.scalatest.FunSuiteLike
import org.scalactic.ConversionCheckedTripleEquals

class IntegrationSpec(_system: ActorSystem) extends TestKit(_system)
    with FunSuiteLike
        with Matchers
    with BeforeAndAfterAll
    with ConversionCheckedTripleEquals
    with ImplicitSender
    with Tools {

  import Replica._
  import Replicator._
  import Arbiter._
  import FakeSecondary._

  def this() = this(ActorSystem("ReplicatorSpec"))

  override def afterAll: Unit = system.shutdown()

  /*
   * Recommendation: write a test case that verifies proper function of the whole system,
   * then run that with flaky Persistence and/or unreliable communication (injected by
   * using an Arbiter variant that introduces randomly message-dropping forwarder Actors).
   */

  test("case1: Primary and secondaries must work in concert when persistence is unreliable") {
    val arbiter = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = true)), "case1-primary")
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(primary, Replicas(Set(primary)))

    client.getAndVerify("k1") //0
    client.setAcked("k1", "v1") //1
    client.getAndVerify("k1") // 2
    client.getAndVerify("k2") // 3
    client.setAcked("k2", "v2") // 4
    client.getAndVerify("k2") // 5
    client.removeAcked("k1") // 6
    client.getAndVerify("k1") // 7

    for (i <- 0 until 100) {
      client.setAcked("k1", s"v$i")
      client.getAndVerify("k1")
      client.removeAcked("k1")
      client.getAndVerify("k1")
    }
  }

  test("case3: Primary and secondaries must work in concert when persistence and communication to secondaries is unreliable") {
    val arbiter = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = true)), "case2-primary")
    val secondary = system.actorOf(Props[FakeSecondary])
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(primary, Replicas(Set(primary, secondary)))

    for (i <- 0 until 50) {
      val key = s"k_$i"
      val setId = client.set(key, s"v$i")
      if (isValid(key)) {
        client.waitAck(setId)
      } else {
        client.waitFailed(setId)
      }
      client.getAndVerify(key)
    }
  }
}


class FakeSecondary extends Actor {
  import Replicator._
  import FakeSecondary._

  def receive = {
    case Snapshot(key, _, seq) =>
      if(isValid(key)) {
        sender() ! SnapshotAck(key, seq)
      }
  }
}

object FakeSecondary {
  def isValid(key: String): Boolean = {
    val items = key.split("_")
    items.length == 2 && items(1).toInt % 3 == 0
  }
}
