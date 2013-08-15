package co.torri.consistenthashing

import scala.collection.{immutable, mutable}
import scala.util.Random


case class Node[Key, Value](id: Int) {

    private[Node] val hash = mutable.HashMap[Key, Value]()

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
        case a: Node[Key, Value] => hash == a.hash
        case _ => false
    }

    def toMap() = 
        hash.toMap

    override def toString = s"Node[${id}] = ${hash}"
}
object Node {
    def apply[Key, Value]() : Node[Key, Value] = apply[Key, Value](Random.nextInt)
}


class Ring[Key, Value](redundancy: Int) {

    def this() = this(3)

    type N = Node[Key, Value]

    private[Ring] var ring = immutable.TreeMap[Int, N]()

    def +(n: N) = {
        n.clear
        val nodes = nodesFor(n)
        ring += ((n.hashCode, n))
        rehash(nodes)
        this
    }

    def -(n: N) = {
        val nodes = nodesFor(n).take(redundancy - 1)
        ring -= n.hashCode
        rehash(nodes)
        this
    }

    def rehash(ns: Seq[N]) : Unit = 
        ns.foreach(rehash(_))

    def rehash(n: N) = 
        n.clear.foreach { case (k,v) => client(k) = v }

    def nodesFor(k: Any) : Seq[N] =
        (ring.from(k.hashCode).toSeq ++ ring.toSeq).take(redundancy).map { case (hash, node) => node }

    override def hashCode() =
        ring.hashCode

    override def equals(a: Any) = a match {
        case r: Ring[Key, Value] => ring == r.ring
        case _ => false
    }

    override def toString() =
        s"""Ring [${ring.values.mkString("\n")}]"""

    object client {

        def -(k: Key) : Unit = nodesFor(k).map(n => n - (k))

        def update(k: Key, v: Value) : Unit = nodesFor(k).map(n => n(k) = v)
    }
}
object Ring {
    def apply[Key, Value]() = new Ring[Key, Value]()
}
