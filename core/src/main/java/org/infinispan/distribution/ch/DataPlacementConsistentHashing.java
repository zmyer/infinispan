package org.infinispan.distribution.ch;

import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.remoting.transport.Address;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The consistent hash function implementation that the Object Lookup implementations from the Data Placement 
 * optimization
 *
 * @author Zhongmiao Li
 * @author João Paiva
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementConsistentHashing extends AbstractConsistentHash {

   ConsistentHash baseHash;
   Map<Address, ObjectLookup> lookUpperList = new HashMap<Address, ObjectLookup>();
   List<Address> addressList = new ArrayList<Address>();

   public void addObjectLookup(Address address, ObjectLookup objectLookup) {
      if (objectLookup == null) {
         return;
      }
      lookUpperList.put(address, objectLookup);
   }

   @Override
   public void setCaches(Set<Address> caches) {
      this.baseHash.setCaches(caches);
   }

   public void setCacheList(List<Address> cacheList) {
      this.addressList = cacheList;
   }

   @Override
   public Set<Address> getCaches() {
      return this.baseHash.getCaches();
   }

   @Override
   public List<Address> locate(Object key, int replCount) {
      List<Address> defaultAddList = baseHash.locate(key, replCount);
      ObjectLookup objectLookup = lookUpperList.get(defaultAddList.get(0));
      int index = objectLookup == null ? ObjectLookup.KEY_NOT_FOUND : objectLookup.query(key);

      if (index == ObjectLookup.KEY_NOT_FOUND) {
         return defaultAddList;
      } else {
         List<Address> addList = new ArrayList<Address>();
         addList.add(addressList.get(index));
         return addList;
      }
   }

   @Override
   public List<Integer> getHashIds(Address a) {
      throw new UnsupportedOperationException("Not yet implemented");
   }

   public void setDefault(ConsistentHash defaultHash) {
      this.baseHash = defaultHash;
   }

   public ConsistentHash getDefaultHash() {
      return this.baseHash;
   }
}
