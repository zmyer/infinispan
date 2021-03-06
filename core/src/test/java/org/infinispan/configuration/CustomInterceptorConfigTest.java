package org.infinispan.configuration;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(testName = "config.CustomInterceptorConfigTest", groups = "functional")
public class CustomInterceptorConfigTest extends AbstractInfinispanTest {

   public void testCustomInterceptors() throws Exception {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<infinispan>" +
            "<cache-container name=\"custom\" default-cache=\"default-cache\">" +
            "<transport />" +
            "<local-cache name=\"default-cache\"><custom-interceptors> \n" +
            "<interceptor after=\""+ InvocationContextInterceptor.class.getName()+"\" class=\""+DummyInterceptor.class.getName()+"\"/> \n" +
            "</custom-interceptors> </local-cache>" +
            "<local-cache name=\"x\">" +
            "<custom-interceptors>\n" +
            "         <interceptor position=\"first\" class=\""+CustomInterceptor1.class.getName()+"\" />" +
            "         <interceptor" +
            "            position=\"last\"" +
            "            class=\""+CustomInterceptor2.class.getName()+"\"" +
            "         />" +
            "</custom-interceptors>" +
            "</local-cache>" +
            "</cache-container>" +
            "</infinispan>";

      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(stream)){
         @Override
         public void call() {
            Cache c = cm.getCache();
            DummyInterceptor i = TestingUtil.findInterceptor(c, DummyInterceptor.class);
            assert i != null;

            Cache<Object, Object> namedCacheX = cm.getCache("x");
            assert TestingUtil.findInterceptor(namedCacheX, CustomInterceptor1.class) != null;
            assert TestingUtil.findInterceptor(namedCacheX, CustomInterceptor2.class) != null;
         }
      });
   }

   public static final class CustomInterceptor1 extends BaseCustomAsyncInterceptor {}
   public static final class CustomInterceptor2 extends BaseCustomAsyncInterceptor {}


   public void testCustomInterceptorsProgramatically() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.customInterceptors().addInterceptor().interceptor(new DummyInterceptor()).position(Position.FIRST);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(cfg)) {
         @Override
         public void call() {
            Cache c = cm.getCache();
            DummyInterceptor i = TestingUtil.findInterceptor(c, DummyInterceptor.class);
            assertNotNull(i);
         }
      });
   }

   public void testCustomInterceptorsProgramaticallyWithOverride() {
      final ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.customInterceptors().addInterceptor().interceptor(new DummyInterceptor()).position(Position.FIRST);
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() {
            cm.defineConfiguration("custom", cfg.build());
            Cache c = cm.getCache("custom");
            DummyInterceptor i = TestingUtil.findInterceptor(c, DummyInterceptor.class);
            assertNotNull(i);
         }
      });

   }

   public static class DummyInterceptor extends BaseCustomAsyncInterceptor {

   }
}
