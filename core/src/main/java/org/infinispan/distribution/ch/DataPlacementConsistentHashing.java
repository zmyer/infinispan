package org.infinispan.distribution.ch;

import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
      this.lookUpperList.put(address, objectLookup);
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
      Integer index = lookUpperList.get(defaultAddList.get(0)).query(key.toString());
      if (index == null)
         return defaultAddList;
      else {
         List<Address> addList = new ArrayList<Address>();
         addList.add(this.addressList.get(index));
         return addList;
      }
   }

   @Override
   public List<Integer> getHashIds(Address a) {
      throw new RuntimeException("Not yet implemented");
   }

   public void setDefault(ConsistentHash defaultHash) {
      this.baseHash = defaultHash;
   }

   public ConsistentHash getDefaultHash() {
      return this.baseHash;
   }
}
