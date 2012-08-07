package org.infinispan.dataplacement.lookup;

/**
 * An interface that is used to query for the new owner index defined by the data placement optimization
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface ObjectLookup {

   public static final int KEY_NOT_FOUND = -1;

   /**
    * queries this object lookup for the node index where the key can be (if the keys is moved)
    *
    * @param key  the key to find
    * @return     the node index where the key is or KEY_NOT_FOUND if the key was not moved
    */
   int query(Object key);
}
