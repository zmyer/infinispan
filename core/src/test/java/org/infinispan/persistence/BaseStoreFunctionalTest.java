package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertArrayEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.commons.util.ByRef;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerStub;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * This is a base functional test class containing tests that should be executed for each cache store/loader
 * implementation. As these are functional tests, they should interact against Cache/CacheManager only and any access to
 * the underlying cache store/loader should be done to verify contents.
 */
@Test(groups = {"unit", "smoke"}, testName = "persistence.BaseStoreFunctionalTest")
public abstract class BaseStoreFunctionalTest extends SingleCacheManagerTest {

   protected abstract PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload);

   protected Object wrap(String key, String value) {
      return value;
   }

   protected String unwrap(Object wrapped) {
      return (String) wrapped;
   }

   protected BaseStoreFunctionalTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void teardown() {
      TestingUtil.clearContent(cacheManager);
      super.teardown();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(false);
   }

   public void testTwoCachesSameCacheStore() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.read(cacheManager.getDefaultCacheConfiguration());
      createCacheStoreConfig(cb.persistence(), false);
      Configuration c = cb.build();
      cacheManager.defineConfiguration("testTwoCachesSameCacheStore-1", c);
      cacheManager.defineConfiguration("testTwoCachesSameCacheStore-2", c);

      Cache<String, Object> first = cacheManager.getCache("testTwoCachesSameCacheStore-1");
      Cache<String, Object> second = cacheManager.getCache("testTwoCachesSameCacheStore-2");

      first.start();
      second.start();

      first.put("key", wrap("key", "val"));
      assertEquals("val", unwrap(first.get("key")));
      assertNull(second.get("key"));

      second.put("key2", wrap("key2", "val2"));
      assertEquals("val2", unwrap(second.get("key2")));
      assertNull(first.get("key2"));
   }

   public void testPreloadAndExpiry() {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence(), true);
      cacheManager.defineConfiguration("testPreloadAndExpiry", cb.build());
      Cache<String, Object> cache = cacheManager.getCache("testPreloadAndExpiry");
      cache.start();

      assert cache.getCacheConfiguration().persistence().preload();

      cache.put("k1", wrap("k1", "v"));
      cache.put("k2", wrap("k2", "v"), 111111, TimeUnit.MILLISECONDS);
      cache.put("k3", wrap("k3", "v"), -1, TimeUnit.MILLISECONDS, 222222, TimeUnit.MILLISECONDS);
      cache.put("k4", wrap("k4", "v"), 333333, TimeUnit.MILLISECONDS, 444444, TimeUnit.MILLISECONDS);

      assertCacheEntry(cache, "k1", "v", -1, -1);
      assertCacheEntry(cache, "k2", "v", 111111, -1);
      assertCacheEntry(cache, "k3", "v", -1, 222222);
      assertCacheEntry(cache, "k4", "v", 333333, 444444);
      cache.stop();

      cache.start();

      assertCacheEntry(cache, "k1", "v", -1, -1);
      assertCacheEntry(cache, "k2", "v", 111111, -1);
      assertCacheEntry(cache, "k3", "v", -1, 222222);
      assertCacheEntry(cache, "k4", "v", 333333, 444444);
   }

   public void testPreloadStoredAsBinary() {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence(), true).memory().storageType(StorageType.BINARY);
      cacheManager.defineConfiguration("testPreloadStoredAsBinary", cb.build());
      Cache<String, Pojo> cache = cacheManager.getCache("testPreloadStoredAsBinary");
      cache.start();

      assert cache.getCacheConfiguration().persistence().preload();
      assertEquals(StorageType.BINARY, cache.getCacheConfiguration().memory().storageType());

      cache.put("k1", new Pojo(1));
      cache.put("k2", new Pojo(2), 111111, TimeUnit.MILLISECONDS);
      cache.put("k3", new Pojo(3), -1, TimeUnit.MILLISECONDS, 222222, TimeUnit.MILLISECONDS);
      cache.put("k4", new Pojo(4), 333333, TimeUnit.MILLISECONDS, 444444, TimeUnit.MILLISECONDS);

      cache.stop();

      cache.start();

      assertEquals(4, cache.entrySet().size());
      assertEquals(new Pojo(1), cache.get("k1"));
      assertEquals(new Pojo(2), cache.get("k2"));
      assertEquals(new Pojo(3), cache.get("k3"));
      assertEquals(new Pojo(4), cache.get("k4"));
   }

   public static class Pojo implements Serializable, ExternalPojo {

      private final int i;

      public Pojo(int i) {
         this.i = i;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Pojo pojo = (Pojo) o;

         return i == pojo.i;

      }

      @Override
      public int hashCode() {
         return i;
      }

   }

   public void testRestoreAtomicMap(Method m) {
      cacheManager.defineConfiguration(m.getName(), configureCacheLoader(null, false).build());
      Cache<String, Object> cache = cacheManager.getCache(m.getName());
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, m.getName());
      map.put("a", "b");

      //evict from memory
      cache.evict(m.getName());

      // now re-retrieve the map
      assertEquals("b", AtomicMapLookup.getAtomicMap(cache, m.getName()).get("a"));
   }

   @Test
   public void testRestoreTransactionalAtomicMap(final Method m) throws Exception {
      cacheManager.defineConfiguration(m.getName(), configureCacheLoader(null, false).build());
      Cache<String, Object> cache = cacheManager.getCache(m.getName());
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      tm.begin();
      final AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, m.getName());
      map.put("a", "b");
      tm.commit();

      //evict from memory
      cache.evict(m.getName());

      // now re-retrieve the map and make sure we see the diffs
      assertEquals("b", AtomicMapLookup.getAtomicMap(cache, m.getName()).get("a"));
   }

   public void testStoreByteArrays(final Method m) throws PersistenceException {
      ConfigurationBuilder base = new ConfigurationBuilder();
      // we need to purge the container when loading, because we could try to compare
      // some old entry using ByteArrayEquivalence and this throws ClassCastException
      // for non-byte[] arguments
      cacheManager.defineConfiguration(m.getName(), configureCacheLoader(base, true).build());
      Cache<byte[], byte[]> cache = cacheManager.getCache(m.getName());
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      cache.put(key, value);
      // Lookup in memory, sanity check
      byte[] lookupKey = {1, 2, 3};
      byte[] found = cache.get(lookupKey);
      assertNotNull(found);
      assertArrayEquals(value, found);
      cache.evict(key);
      // Lookup in cache store
      found = cache.get(lookupKey);
      assertNotNull(found);
      assertArrayEquals(value, found);
   }

   public void testRemoveCache() {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence(), true);
      EmbeddedCacheManager local = TestCacheManagerFactory.createCacheManager(cb);
      try {
         final String cacheName = "to-be-removed";
         local.defineConfiguration(cacheName, local.getDefaultCacheConfiguration());
         Cache<String, Object> cache = local.getCache(cacheName);
         assertTrue(local.isRunning(cacheName));
         cache.put("1", wrap("1", "v1"));
         assertCacheEntry(cache, "1", "v1", -1, -1);
         local.administration().removeCache(cacheName);
         assertFalse(local.isRunning(cacheName));
      } finally {
         TestingUtil.killCacheManagers(local);
      }
   }

   public void testRemoveCacheWithPassivation() {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence().passivation(true), true);
      EmbeddedCacheManager local = TestCacheManagerFactory.createCacheManager(cb);
      try {
         final String cacheName = "to-be-removed";
         local.defineConfiguration(cacheName, local.getDefaultCacheConfiguration());
         Cache<String, Object> cache = local.getCache(cacheName);
         assertTrue(local.isRunning(cacheName));
         cache.put("1", wrap("1", "v1"));
         assertCacheEntry(cache, "1", "v1", -1, -1);
         ByRef<Boolean> passivate = new ByRef<>(false);
         PersistenceManager actual = cache.getAdvancedCache().getComponentRegistry().getComponent(PersistenceManager.class);
         PersistenceManager stub = new PersistenceManagerStub() {
            @Override
            public void stop() {
               actual.stop();
            }

            @Override
            public void writeBatchToAllNonTxStores(Iterable<MarshallableEntry> entries,
                  Predicate<? super StoreConfiguration> predicate, long flags) {
               passivate.set(true);
            }
         };
         TestingUtil.replaceComponent(cache, PersistenceManager.class, stub, true);
         local.administration().removeCache(cacheName);
         assertFalse(local.isRunning(cacheName));
         assertFalse(passivate.get());
         assertEquals(0, actual.size());
      } finally {
         TestingUtil.killCacheManagers(local);
      }
   }

   public void testPutAllBatch() throws Exception {
      int numberOfEntries = 100;
      String cacheName = "testPutAllBatch";
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.read(cacheManager.getDefaultCacheConfiguration());
      createCacheStoreConfig(cb.persistence(), false);
      cacheManager.defineConfiguration(cacheName, cb.build());

      Cache<String, Object> cache = cacheManager.getCache(cacheName);
      Map<String, Object> entriesMap = IntStream.range(0, numberOfEntries).boxed()
            .collect(Collectors.toMap(Object::toString, i -> wrap(i.toString(), "Val"+i)));
      cache.putAll(entriesMap);

      assertEquals(numberOfEntries, cache.size());
      CacheLoader cl = TestingUtil.getCacheLoader(cache);
      if (cl != null)
         IntStream.range(0, numberOfEntries).forEach(i -> assertNotNull(cl.loadEntry(Integer.toString(i))));
   }

   public void testLoadEntrySet() {
      int numberOfEntries = 10;
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence(), true);
      cacheManager.defineConfiguration("testLoadKeySet", cb.build());
      Cache<String, Object> cache = cacheManager.getCache("testLoadKeySet");

      Map<String, Object> entriesMap = IntStream.range(0, numberOfEntries).boxed()
            .collect(Collectors.toMap(Object::toString, i -> wrap(i.toString(), "Val"+i)));
      cache.putAll(entriesMap);

      cache.stop();
      cache.start();

      CacheSet<Map.Entry<String, Object>> set = cache.entrySet();
      assertEquals(numberOfEntries, cache.size());
      assertEquals(numberOfEntries, set.size());
      set.forEach(e -> assertEquals(entriesMap.get(e.getKey()), e.getValue()));
   }

   public void testReloadWithEviction() {
      int numberOfEntries = 10;
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence(), false).memory().size(numberOfEntries/2).evictionType(EvictionType.COUNT);
      cacheManager.defineConfiguration("testReload", cb.build());
      Cache<String, Object> cache = cacheManager.getCache("testReload");

      Map<String, Object> entriesMap = IntStream.range(0, numberOfEntries).boxed()
            .collect(Collectors.toMap(Object::toString, i -> wrap(i.toString(), "Val"+i)));
      cache.putAll(entriesMap);

      assertEquals(numberOfEntries, cache.size());
      entriesMap.forEach((k, v) -> assertEquals(v, cache.get(k)));

      cache.stop();
      cache.start();

      assertEquals(numberOfEntries, cache.size());
      entriesMap.forEach((k, v) -> assertEquals(v, cache.get(k)));
   }

   private ConfigurationBuilder configureCacheLoader(ConfigurationBuilder base, boolean purge) {
      ConfigurationBuilder cfg = base == null ? new ConfigurationBuilder() : base;
      cfg.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      createCacheStoreConfig(cfg.persistence(), false);
      cfg.persistence().stores().get(0).purgeOnStartup(purge);
      return cfg;
   }

   private void assertCacheEntry(Cache cache, String key, String value, long lifespanMillis, long maxIdleMillis) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assertNotNull(ice);
      assertEquals(value, unwrap(ice.getValue()));
      assertEquals(lifespanMillis, ice.getLifespan());
      assertEquals(maxIdleMillis, ice.getMaxIdle());
      if (lifespanMillis > -1) assert ice.getCreated() > -1 : "Lifespan is set but created time is not";
      if (maxIdleMillis > -1) assert ice.getLastUsed() > -1 : "Max idle is set but last used is not";
   }
}
