package com.hazelcast.spark.connector.util

import javax.cache.{Cache, CacheManager}

import com.hazelcast.cache.impl.{CacheProxy, HazelcastServerCachingProvider}
import com.hazelcast.client.cache.impl.{ClientCacheProxy, HazelcastClientCachingProvider}
import com.hazelcast.client.proxy.ClientMapProxy
import com.hazelcast.config.CacheConfig
import com.hazelcast.core.{HazelcastInstance, IMap}
import com.hazelcast.map.impl.proxy.MapProxyImpl

import scala.reflect.ClassTag

object HazelcastUtil {

  def getClientMapProxy[K, V](name: String, instance: HazelcastInstance): ClientMapProxy[K, V] = {
    val map: IMap[K, V] = instance.getMap(name)
    map.asInstanceOf[ClientMapProxy[K, V]]
  }

  def getServerMapProxy[K, V](name: String, instance: HazelcastInstance): MapProxyImpl[K, V] = {
    val map: IMap[K, V] = instance.getMap(name)
    map.asInstanceOf[MapProxyImpl[K, V]]
  }

  def getClientCacheProxy[K, V](name: String, instance: HazelcastInstance): ClientCacheProxy[K, V] = {
    val cachingProvider: HazelcastClientCachingProvider = HazelcastClientCachingProvider.createCachingProvider(instance)
    val cacheManager: CacheManager = cachingProvider.getCacheManager()
    val cacheConfig: CacheConfig[K, V] = new CacheConfig[K, V](name)
    val cache: Cache[K, V] = cacheManager.createCache(name, cacheConfig)
    cache.asInstanceOf[ClientCacheProxy[K, V]]
  }

  def getServerCacheProxy[K, V](name: String, instance: HazelcastInstance): CacheProxy[K, V] = {
    val cachingProvider: HazelcastServerCachingProvider = HazelcastServerCachingProvider.createCachingProvider(instance)
    val cacheManager: CacheManager = cachingProvider.getCacheManager()
    val cacheConfig: CacheConfig[K, V] = new CacheConfig[K, V](name)
    val cache: Cache[K, V] = cacheManager.createCache(name, cacheConfig)
    cache.asInstanceOf[CacheProxy[K, V]]
  }

  def getClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

}
