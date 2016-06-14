package com.hazelcast.spark.connector.iterator

import java.util
import java.util.Map.Entry

class MapIterator[K, V](val iterator: util.Iterator[Entry[K, V]]) extends Iterator[(K, V)] {
  override def hasNext: Boolean = iterator.hasNext

  override def next(): (K, V) = {
    val entry: Entry[K, V] = iterator.next()
    (entry.getKey, entry.getValue)
  }
}
