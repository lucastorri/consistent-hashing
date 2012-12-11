package co.torri.consistenthashing

import scala.collection.{immutable, mutable}
import scala.util.Random


case class Node[Key, Value](id: Int) {

    val hash = mutable.HashMap[Key, Value]()

    def clear = {
        val old = hash.clone
        hash.clear
        old
    }

    def update(k: Key, v: Value) = hash(k) = v

    def -(k: Key) : (Key, Value) = {
        val v = hash(k)
        hash -= (k)
        (k, v)
    }

    override def hashCode = id

    override def equals(a: Any) = a match {
        case a: AnyRef => a eq this
        case _ => false
    }

    override def toString = "Node["+id+"] = " + hash
}
object Node {
    def apply[Key, Value]() : Node[Key, Value] = apply[Key, Value](Random.nextInt)
}


class Ring[Key, Value](redundancy: Int) {

    def this() = this(3)

    type N = Node[Key, Value]

    var ring = immutable.TreeMap[Int, N]()

    def +(n: N) = {
        n.clear
        val nodes = nodesFor(n)
        ring += ((n.hashCode, n))
        rehash(nodes)
        this
    }

    def -(n: N) = {
        ring -= n.hashCode
        rehash(nodesFor(n).take(redundancy - 1))
        this
    }

    def rehash(ns: Seq[N]) : Unit = ns.foreach(rehash(_))

    def rehash(n: N) = n.clear.foreach { case (k,v) => client(k) = v }

    def nodesFor(k: Any) : Seq[N] =
        (ring.from(k.hashCode).toSeq ++ ring.toSeq).take(redundancy).map { case (hash, node) => node }

    object client {

        def -(k: Key) : Unit = nodesFor(k).map(n => n - (k))

        def update(k: Key, v: Value) : Unit = nodesFor(k).map(n => n(k) = v)
    }
}
object Ring {
    def apply[Key, Value]() = new Ring[Key, Value]()
}

object main {

    val r = Ring[Int, String]()
    var n0 = Node[Int, String](0)
    var n1 = Node[Int, String](1000)
    var n2 = Node[Int, String](2000)
    var n3 = Node[Int, String](3000)
    var n4 = Node[Int, String](4000)
    var n5 = Node[Int, String](5000)
    var n6 = Node[Int, String](6000)
    var n7 = Node[Int, String](7000)
    var n8 = Node[Int, String](8000)

    val c = r.client
    def p = r.ring.foreach(println)

    def main = {
        r + n0 + n1 + n2 + n3 + n4 + n5
        c(0) = "oi"
        p
    }
}