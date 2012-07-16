package org.infinispan.dataplacement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.dataplacement.DataPlacementRequestCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.MachineLearningConsistentHashing;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.DistributedStateTransferManagerImpl;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class RequestManager {
	private StreamLibContainer analyticsBean;
		private static final Log log = LogFactory
			.getLog(RequestManager.class);
		
	private List<Address> addressList;
	private Set<Object> movedInList = new HashSet<Object>();
	private Set<Object> requestSentList = new HashSet<Object>();
	private Integer requestRound = 0;
	
	private CommandsFactory commandsFactory;
	private RpcManager rpcManager;
	private DistributionManager distributionManager;
	private DistributedStateTransferManagerImpl stateTransfer;
	private String cacheName;
	private DataContainer dataContainer;
	private DataPlacementManager dataPlacementManager;
	

	//private List<Pair<Integer, Map<Object, Long>>> requestReceivedList = new ArrayList<Pair<Integer, Map<Object, Long>>>();
	
	
	private TestWriter writer = TestWriter.getInstance();
	
	public RequestManager(CommandsFactory commandsFactory, RpcManager rpcManager, DistributionManager distributionManager, 
			DistributedStateTransferManagerImpl  stateTransfer, DataContainer dataContainer,String cacheName, DataPlacementManager dataPlacementManager) {
		this.commandsFactory = commandsFactory;
		this.rpcManager = rpcManager;
		this.distributionManager = distributionManager;
		this.stateTransfer = stateTransfer;
		this.cacheName = cacheName;
		this.dataContainer = dataContainer;
		this.dataPlacementManager = dataPlacementManager;
		analyticsBean = StreamLibContainer.getInstance();
	}

	public Map<Object,Long> sendRequestToAll(){
		Map<Object, Long> remoteGet = analyticsBean
				.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
						analyticsBean.getCapacity());
		
		log.info("Size of Remote Get is :" + remoteGet.size());
		// remotePut =
		// analyticsBean.getTopKFrom(Anal	yticsBean.Stat.REMOTE_PUT,analyticsBean.getCapacity());

		// Only send statistics if there are enough objects
		if (remoteGet.size() >= analyticsBean.getCapacity() * 0.8) {
			Map<Object, Long> localGet = getStasticsForMovedInObj();
			log.info("Size of movedin objects:"+localGet.size());
			
			Map<Address, Map<Object, Long>> tmpList = sortObjectsByOwner(remoteGet,true);
			for(Entry<Address,Map<Object, Long>> map : tmpList.entrySet()){
				log.info("Sorting remote list:"+map.getKey() +": Size of list"+ map.getValue().size());
			}
			
			remoteGet.putAll(localGet);
			requestSentList = remoteGet.keySet();
			log.info("Merged list size:"+remoteGet.size());
			Map<Address, Map<Object, Long>> topGetPerOwnerList = sortObjectsByOwner(remoteGet,true);

			for (Entry<Address, Map<Object, Long>> entry : topGetPerOwnerList
					.entrySet()) {
				this.sendRequest(entry.getKey(), entry.getValue());
			}
			
			//If there is no request to a node, still send one empty request
			List<Address> addresses = this.rpcManager.getTransport().getMembers();
			for (Address add : addresses) {
				if (topGetPerOwnerList.containsKey(add) == false
						&& !add.equals(rpcManager.getAddress())) {
					this.sendRequest(add, new HashMap<Object, Long>());
				}
			}
			
			if(topGetPerOwnerList.containsKey(rpcManager.getAddress())){
				log.info("Returning request of my own");
				return topGetPerOwnerList.get(rpcManager.getAddress());
			}
			else{
				log.info("Returning empty request");
				return new HashMap<Object, Long>();
			}
		}
		return null;
	}
	
	public void increaseRoundID(){
		++this.requestRound;
	}
	
	private void sendRequest(Address owner, Map<Object, Long> remoteTopList) {

		DataPlacementRequestCommand command = commandsFactory
				.buildDataPlacementRequestCommand();
		command.init(dataPlacementManager, this.distributionManager);
		command.putRemoteList(remoteTopList, this.requestRound);
		if (!this.rpcManager.getAddress().toString().equals(owner)) {
			try {
				this.rpcManager.invokeRemotely(Collections.singleton(owner),
						command, false);
				log.info("Putting Message with size " + remoteTopList.size()+" to "+ owner);
				writer.write(true, null, remoteTopList);
			} catch (Throwable throwable) {
				log.error(throwable.toString());
			}
		} else {
			log.warn("Message will not be sent to myself!");
		}
	}
	
	
	public Map<Object, Long> getStasticsForMovedInObj(){
		Map<Object, Long> localGetRealStatics = this.analyticsBean
				.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET,
						this.analyticsBean.getCapacity());
		
		Map<Object, Long> localGetEstStatics = new HashMap<Object,Long>();
		List<Object> tempList = new ArrayList<Object>();
		Long minAccess = Long.MAX_VALUE;
		for(Object key: movedInList){
			Long accessNum = localGetRealStatics.get(key);
			if( accessNum != null){
				localGetEstStatics.put(key, accessNum);
				if(accessNum < minAccess)
					minAccess = accessNum;
			}
			else
				tempList.add(key);
		}
		
		log.info("Movedin Object list size :"+movedInList.size());
		log.info("Min Access Num is:"+minAccess);
		log.info("Number of objects moved in but not in local top list:"+tempList.size());
		for(Object key : tempList){
			localGetEstStatics.put(key, minAccess);
		}
		
		return localGetEstStatics;
	}
	
	
	private Map<Address, Map<Object, Long>> sortObjectsByOwner(
			Map<Object, Long> remoteGet, boolean useDefaultHash) {
		Map<Address, Map<Object, Long>> objectLists = new HashMap<Address, Map<Object, Long>>();
		Map<Object, List<Address>> mappedObjects = getConsistentHashingFunction(useDefaultHash).locateAll(remoteGet.keySet(), 1);

		Address addr = null;
		Object key = null;

		for (Entry<Object, Long> entry : remoteGet.entrySet()) {
			key = entry.getKey();
			addr = mappedObjects.get(key).get(0);

			if (!objectLists.containsKey(addr)) {
				objectLists.put(addr, new HashMap<Object, Long>());
			}
			objectLists.get(addr).put(entry.getKey(), entry.getValue());
		}
		
		log.info("Own number of movedin object after sorting:"+objectLists.size());
		
		log.info("MLHASH: List Sorting: Number of owner"+ objectLists.size());
		for(Entry<Address,Map<Object, Long>> map : objectLists.entrySet()){
			log.info(map.getKey() +": Size of list"+ map.getValue().size());
		}
		

		return objectLists;
	}
	
	public void postPhaseTest(){
		
		log.info("Request List Size: "+requestSentList.size());
		log.info("Movedin List Size: "+ movedInList.size());
		
		log.info("Adding moved-in list");
		
		log.info("Moved In List size before deleting is:"+movedInList.size());
		
		Set<Object> tempSet = new HashSet<Object>();
		//Add moved-in keys into the list
		for (Object key : movedInList){	
			if(dataContainer.containsKey(key)){
				tempSet.add(key);
			}
		}
		movedInList = tempSet;
		
		log.info("Moved In List size after deleting is:"+ movedInList.size());
		
		//Add moved-in keys into the list
		for (Object key : requestSentList){	
			if(dataContainer.containsKey(key)){
				movedInList.add(key);
			}
		}
		log.info("Moved In List size after merge is:"+movedInList.size());
		analyticsBean.resetAll();
		log.info(analyticsBean
				.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
						analyticsBean.getCapacity()).size());
		
		log.info(analyticsBean
				.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET,
						analyticsBean.getCapacity()).size());
		
		log.info("Request Sent List:"+requestSentList.toString());
	}

	
	public ConsistentHash getConsistentHashingFunction(boolean useDefaultHash) {
	ConsistentHash hash = this.distributionManager.getConsistentHash();
	if (hash instanceof MachineLearningConsistentHashing){
		if(useDefaultHash){
			log.info("Returning default hash of MLHash");
		   return ((MachineLearningConsistentHashing) hash).getDefaultHash();
		}
	     else{
	    	 log.info("Returning MLHash");
		   return hash;
	     }
	}
	else{
		if(useDefaultHash){
			log.info("Returning default hash");
		}
	     else{
	    	 log.info("No MLHashing!");
	     }
		log.info("Returning normal hash");
		return hash;
	}
    }

	public Integer getRoundID() {
		return requestRound;
	}
}
