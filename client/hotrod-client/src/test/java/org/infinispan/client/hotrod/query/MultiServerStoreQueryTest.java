package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.Objects;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.testng.annotations.Test;

/**
 * Reproducer for https://issues.jboss.org/browse/ISPN-9068
 */
@Test(testName = "client.hotrod.query.MultiServerStoreQueryTest", groups = "functional")
public class MultiServerStoreQueryTest extends MultiHotRodServersTest {

   private static final int NODES = 2;
   private static final boolean USE_PERSISTENCE = true;

   private static final String USER_CACHE = "news";
   private static final String LUCENE_LOCKING_CACHE = "LuceneIndexesLocking_news";
   private static final String LUCENE_METADATA_CACHE = "LuceneIndexesMetadata_news";
   private static final String LUCENE_DATA_CACHE = "LuceneIndexesData_news";

   private RemoteCache<Object, Object> userCache;

   public Configuration getLockCacheConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false).build();
   }

   public Configuration getLuceneCacheConfig(String storeName) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      if (USE_PERSISTENCE) {
         builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true).storeName(storeName);
      }
      return builder.build();
   }

   public Configuration buildIndexedConfig(String storeName) {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.indexing().index(Index.PRIMARY_OWNER)
            .addProperty("default.indexmanager", InfinispanIndexManager.class.getName())
            .addProperty("default.worker.execution", "async")
            .addProperty("default.index_flush_interval", "500")
            .addProperty("default.indexwriter.merge_factor", "30")
            .addProperty("default.indexwriter.merge_max_size", "1024")
            .addProperty("default.indexwriter.ram_buffer_size", "256")
            .addProperty("default.locking_cachename", LUCENE_LOCKING_CACHE)
            .addProperty("default.data_cachename", LUCENE_DATA_CACHE)
            .addProperty("default.metadata_cachename", LUCENE_METADATA_CACHE);
      if (USE_PERSISTENCE)
         builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true).storeName(storeName);

      return builder.build();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      createHotRodServers(NODES, defaultConfiguration);

      for (int i = 0; i < cacheManagers.size(); i++) {
         EmbeddedCacheManager cm = cacheManagers.get(i);
         cm.defineConfiguration(USER_CACHE, buildIndexedConfig("News-" + i));

         cm.defineConfiguration(LUCENE_LOCKING_CACHE, getLockCacheConfig());
         cm.defineConfiguration(LUCENE_METADATA_CACHE, getLuceneCacheConfig(LUCENE_METADATA_CACHE + "_" + i));
         cm.defineConfiguration(LUCENE_DATA_CACHE, getLuceneCacheConfig(LUCENE_DATA_CACHE + "_" + i));
         cm.getCache(USER_CACHE);
      }

      waitForClusterToForm(USER_CACHE);

      RemoteCacheManager remoteCacheManager = client(0);
      userCache = remoteCacheManager.getCache(USER_CACHE);

      //initialize client-side serialization context
      SerializationContext serializationContext = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String protoFile = protoSchemaBuilder.fileName("news.proto")
            .addClass(News.class)
            .addClass(NewsKey.class)
            .build(serializationContext);

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("news.proto", protoFile);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      return super.createHotRodClientConfigurationBuilder(host, serverPort)
            .marshaller(new ProtoStreamMarshaller());
   }

   public void testIndexing() {
      News news = new News();
      news.setId("testnews");
      news.setTimestamp(0);

      userCache.put(news.getId(), news);

      Object testNews = userCache.get("testnews");
      assertEquals(news, testNews);
   }

   public void testNonPrimitiveKey() {
      NewsKey newsKey1 = new NewsKey();
      newsKey1.setArticle("articleKey1");

      NewsKey newsKey2 = new NewsKey();
      newsKey2.setArticle("articleKey2");

      News news1 = new News();
      news1.setId("test-news-1");
      news1.setTimestamp(0);

      News news2 = new News();
      news2.setId("test-news-2");
      news2.setTimestamp(0);

      userCache.put(newsKey1, news1);
      userCache.put(newsKey2, news2);

      assertEquals(news1, userCache.get(newsKey1));
      assertEquals(news2, userCache.get(newsKey2));
   }

}

class NewsKey {
   private String article;

   NewsKey() {
   }

   @ProtoField(number = 1, required = true)
   public void setArticle(String article) {
      this.article = article;
   }

   public String getArticle() {
      return article;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NewsKey newsKey = (NewsKey) o;
      return Objects.equals(article, newsKey.article);
   }

   @Override
   public int hashCode() {
      return Objects.hash(article);
   }
}

@ProtoDoc("@Indexed")
class News {
   private String id;
   private long timestamp;

   public News() {
   }

   public String getId() {
      return id;
   }

   @ProtoField(number = 1, required = true)
   public void setId(String id) {
      this.id = id;
   }

   @ProtoDoc("@Field(index=Index.YES, analyze = Analyze.NO, store = Store.NO)")
   @ProtoDoc("@SortableField")
   @ProtoField(number = 2, required = true)
   public long getTimestamp() {
      return timestamp;
   }

   public void setTimestamp(long time) {
      this.timestamp = time;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      News news = (News) o;
      return timestamp == news.timestamp && Objects.equals(id, news.id);
   }

   @Override
   public int hashCode() {
      return Objects.hash(id, timestamp);
   }
}
