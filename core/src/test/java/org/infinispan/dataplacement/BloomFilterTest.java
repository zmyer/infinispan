package org.infinispan.dataplacement;

import org.infinispan.dataplacement.c50.lookup.BloomFilter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "dataplacement.BloomFilterTest")
public class BloomFilterTest {
   
   private static final Log log = LogFactory.getLog(BloomFilterTest.class);
   
   private static final int START = 10;
   private static final int END = 10000000;
   private static final double PROB = 0.001;
   
   public void testBloomFilter() {
      
      for (int iteration = START; iteration <= END; iteration *= 10) {
         BloomFilter bloomFilter = new BloomFilter(PROB, iteration);
         for (int key = 0; key < iteration; ++key) {
            bloomFilter.add(getKey(key));
         }
         
         log.infof("=== Iterator with %s objects ===", iteration);
         log.infof("Bloom filter size = %s", bloomFilter.size());
         
         long begin = System.currentTimeMillis();
         for (int key = 0; key < iteration; ++key) {
            assert bloomFilter.contains(getKey(key)) : "False Negative should not happen!";
         }
         long end = System.currentTimeMillis();
         
         log.infof("Query duration:\n\ttotal=%s ms\n\tper-key=%s ms", end - begin, (end - begin) / iteration);
      }
      
   }
   
   private String getKey(int index) {
      return "KEY_" + index;
   }
   
}
