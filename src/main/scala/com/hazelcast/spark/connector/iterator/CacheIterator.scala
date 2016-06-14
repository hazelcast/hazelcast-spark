package com.hazelcast.spark.connector.iterator

import java.util
import javax.cache.Cache.Entry

class CacheIterator[K, V](val iterator: util.Iterator[Entry[K, V]]) extends Iterator[(K, V)] {
  override def hasNext: Boolean = iterator.hasNext

  override def next(): (K, V) = {
    val entry: Entry[K, V] = iterator.next()
    (entry.getKey, entry.getValue)
  }
}
