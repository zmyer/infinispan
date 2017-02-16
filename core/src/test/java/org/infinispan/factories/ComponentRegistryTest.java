package org.infinispan.factories;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test the concurrent lookup of components for ISPN-2796.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "factories.ComponentRegistryTest")
public class ComponentRegistryTest extends AbstractInfinispanTest {
   private GlobalComponentRegistry gcr;
   private ComponentRegistry cr1;
   private ComponentRegistry cr2;
   private TestDelayFactory.Control control;

   @BeforeMethod
   public void setUp() throws InterruptedException, ExecutionException {
      GlobalConfiguration gc = new GlobalConfigurationBuilder().build();
      Configuration c = new ConfigurationBuilder().build();
      Set<String> cachesSet = new HashSet<String>();
      EmbeddedCacheManager cm = mock(EmbeddedCacheManager.class);
      AdvancedCache cache = mock(AdvancedCache.class);

      gcr = new GlobalComponentRegistry(gc, cm, cachesSet);
      cr1 = new ComponentRegistry("cache", c, cache, gcr, ComponentRegistryTest.class.getClassLoader());
      cr2 = new ComponentRegistry("cache", c, cache, gcr, ComponentRegistryTest.class.getClassLoader());

      control = new TestDelayFactory.Control();
      gcr.registerComponent(control, TestDelayFactory.Control.class);
   }

   public void testSingleThreadLookup() throws InterruptedException, ExecutionException {
      control.unblock();

      TestDelayFactory.Component c1 = cr1.getOrCreateComponent(TestDelayFactory.Component.class);
      assertNotNull(c1);

      TestDelayFactory.Component c2 = cr1.getOrCreateComponent(TestDelayFactory.Component.class);
      assertNotNull(c2);
   }

   public void testConcurrentLookupSameComponentRegistry() throws InterruptedException, ExecutionException {

      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            return cr1.getOrCreateComponent(TestDelayFactory.Component.class);
         }
      });

      // getComponent doesn't wait for the getOrCreateComponent call on the forked thread to finish
      // It returns null instead, but that's ok because getComponent doesn't guarantee anything
      Thread.sleep(500);
      assertNull(cr1.getComponent(TestDelayFactory.Component.class));

      control.unblock();
      assertNotNull(future.get());
      // now that getOrCreateComponent has finished, getComponent works as well
      assertNotNull(cr1.getComponent(TestDelayFactory.Component.class));
   }

   public void testConcurrentLookupDifferentComponentRegistries() throws InterruptedException, ExecutionException {

      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            return cr1.getOrCreateComponent(TestDelayFactory.Component.class);
         }
      });

      Thread.sleep(500);
      assertNotNull(cr2.getOrCreateComponent(TestDelayFactory.Component.class));

      control.unblock();
      assertNotNull(future.get());
   }
}
