package org.infinispan.distribution.wrappers;

import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
@Test (groups = "functional", testName = "distribution.CustomInterceptorTest")
public class CustomInterceptorTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration dcc = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      dcc.fluent().customInterceptors().add(new CustomStatsInterceptor()).first();
      createCluster(dcc, 2);
   }

   public void testCustomInterceptor() {
      cache(0).put("k", "v");
      assertEquals(cache(0).get("k"), "v");
      assertEquals(cache(1).get("k"), "v");
   }
}
