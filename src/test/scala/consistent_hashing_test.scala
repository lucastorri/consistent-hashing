package co.torri.consistenthashing

import org.scalatest.FlatSpec
import org.scalatest.matchers.MustMatchers

class RingTest extends FlatSpec with MustMatchers {

  it must "store items redundantly on the ring" in new context with baseRing {

    client(0) = "hi"
    client(45) = "hello"

    r must equal (ring(
      node(0, 0 -> "hi", 45 -> "hello"),
      node(30, 0 -> "hi"),
      node(50, 0 -> "hi", 45 -> "hello"),
      node(70, 45 -> "hello")
    ))
  }

  it must "rehash the ring when a node is added" in new context with baseRing {

    client(25) = "hi"
    client(45) = "hello"
    client(55) = "hey"
    r + nodes(60)

    r must equal (ring(
      node(0, 55 -> "hey"),
      node(30, 25 -> "hi"),
      node(50, 25 -> "hi", 45 -> "hello"),
      node(60, 25 -> "hi", 45 -> "hello", 55 -> "hey"),
      node(70, 45 -> "hello", 55 -> "hey")
    ))
  }

  it must "rehash the ring when a node is removed" in new context with baseRing {

    r + nodes(20)

    client(15) = "hi"
    client(45) = "hello"
    r - nodes(50)

    r must equal(ring(
      node(0, 45 -> "hello"),
      node(20, 15 -> "hi", 45 -> "hello"),
      node(30, 15 -> "hi"),
      node(70, 15 -> "hi", 45 -> "hello")
    ))
  }

  trait context {
    val r = Ring[Int, String]()
    val nodes = (0 to 9)
      .map { i => 
        val id = i * 10
        (id, Node[Int, String](id))
      }
      .toMap
    
    val client = r.client
  }

  trait baseRing { self: context =>
    r + nodes(0) + nodes(30) + nodes(70) + nodes(50)
  }

  def ring[K, V](nodes: Node[K, V]*) = {
    val r = Ring[K, V]()
    nodes.foreach { n =>
      val nn = Node[K, V](n.id)
      r + nn
      n.toMap.foreach { case (k, v) =>
        nn(k) = v
      }
    }
    r
  }

  def node[K, V](id: Int, values: (K, V)*) = {
    val n = Node[K, V](id)
    values.foreach { case (k, v) =>
      n(k) = v
    }
    n
  }

}