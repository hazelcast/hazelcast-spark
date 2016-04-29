package com.hazelcast.spark.connector

import javax.cache.{Cache, CacheManager}

import com.hazelcast.cache.impl.{CacheProxy, HazelcastServerCachingProvider}
import com.hazelcast.client.cache.impl.{ClientCacheProxy, HazelcastClientCachingProvider}
import com.hazelcast.config.CacheConfig
import com.hazelcast.config.EvictionConfig.MaxSizePolicy._
import com.hazelcast.core.HazelcastInstance

object HazelcastHelper {

  def getCacheFromClientProvider[K, V](name: String, instance: HazelcastInstance): ClientCacheProxy[K, V] = {
    val cachingProvider: HazelcastClientCachingProvider = HazelcastClientCachingProvider.createCachingProvider(instance)
    val cacheManager: CacheManager = cachingProvider.getCacheManager()
    val cacheConfig: CacheConfig[K, V] = new CacheConfig[K, V](name)
    cacheConfig.getEvictionConfig.setMaximumSizePolicy(ENTRY_COUNT).setSize(Int.MaxValue)
    val cache: Cache[K, V] = cacheManager.createCache(name, cacheConfig)
    cache.asInstanceOf[ClientCacheProxy[K, V]]
  }

  def getCacheFromServerProvider[K, V](name: String, instance: HazelcastInstance): CacheProxy[K, V] = {
    val cachingProvider: HazelcastServerCachingProvider = HazelcastServerCachingProvider.createCachingProvider(instance)
    val cacheManager: CacheManager = cachingProvider.getCacheManager()
    val cacheConfig: CacheConfig[K, V] = new CacheConfig[K, V](name)
    cacheConfig.getEvictionConfig.setMaximumSizePolicy(ENTRY_COUNT).setSize(Int.MaxValue)
    val cache: Cache[K, V] = cacheManager.createCache(name, cacheConfig)
    cache.asInstanceOf[CacheProxy[K, V]]
  }

}
