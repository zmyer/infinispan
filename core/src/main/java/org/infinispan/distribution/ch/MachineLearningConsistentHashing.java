package org.infinispan.distribution.ch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.dataplacement.lookup.ObjectLookUpper;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The hash function that contains machine learning rules and bloom filter
 * @author Zhongmiao Li
 *
 */
public class MachineLearningConsistentHashing extends AbstractConsistentHash{
    
	private static final Log LOG = LogFactory.getLog(MachineLearningConsistentHashing.class);
	ConsistentHash baseHash;
	Map<Address, ObjectLookUpper> lookUpperList = new HashMap<Address, ObjectLookUpper>();
	List<Address> addressList = new ArrayList<Address>();
	//Set<Address> addressSet;
	
	
	public void setLookUpper(Address address, ObjectLookUpper lookupper){
		lookUpperList.put(address, lookupper);
	}
	
	@Override
	public void setCaches(Set<Address> caches) {
		baseHash.setCaches(caches);
	}
	
	public void setCacheList(List<Address> cacheList){
		addressList = cacheList;
	}

	@Override
	public Set<Address> getCaches() {
		return baseHash.getCaches();
	}

	@Override
	public List<Address> locate(Object key, int replCount) {
		 List<Address> defaultAddList = baseHash.locate(key, replCount);
		 Integer index = lookUpperList.get(defaultAddList.get(0)).query(key.toString());
		 if(index == null){
			 return defaultAddList;
		 }
		 else{
			 List<Address> addList = new ArrayList<Address>();
			 addList.add(addressList.get(index));
			 return addList;
		 }
	}

	@Override
	public List<Integer> getHashIds(Address a) {
		throw new RuntimeException("Not yet implemented");
	}

	public void setDefault(ConsistentHash defaultHash) {
		baseHash =  defaultHash;
	}

}
