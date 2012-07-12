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
		
	private Set<Object> requestSentList = new HashSet<Object>();
	private CommandsFactory commandsFactory;
	private RpcManager rpcManager;
	private Integer requestRound = 0, replyRound = 0;
	private DistributionManager distributionManager;
	private DistributedStateTransferManagerImpl stateTransfer;
	private String cacheName;
	private List<Address> addressList;
	private DataContainer dataContainer;
	private DataPlacementManager dataPlacementManager;
	private Set<Object> movedInList = new HashSet<Object>();
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

	public void sendRequestToAll(){
		Map<Object, Long> remoteGet = analyticsBean
				.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
						analyticsBean.getCapacity());
		
		// remotePut =
		// analyticsBean.getTopKFrom(Anal	yticsBean.Stat.REMOTE_PUT,analyticsBean.getCapacity());

		// Only send statistics if there are enough objects
		if (remoteGet.size() >= analyticsBean.getCapacity() * 0.8) {
			
			Map<Object, Long> localGet = getStasticsForMovedInObj();
			log.info("Moved In Object Size:"+localGet.size());
			remoteGet.putAll(localGet);
			log.info("Merged List Size:"+remoteGet.size());
			Map<Address, Map<Object, Long>> topGetPerOwnerList = sortObjectsByOwner(remoteGet);
			
			requestSentList = remoteGet.keySet();
			
			log.info("Size of list is ");

			for (Entry<Address, Map<Object, Long>> entry : topGetPerOwnerList
					.entrySet()) {
				this.sendRequest(entry.getKey(), entry.getValue());
			}
			List<Address> addresses = this.rpcManager.getTransport().getMembers();
			for (Address add : addresses) {
				if (topGetPerOwnerList.containsKey(add) == false
						&& add.equals(this.cacheName)) {
					this.sendRequest(add, new HashMap<Object, Long>());
				}
			}
			++this.requestRound;
		}
	}
	
	private void sendRequest(Address owner, Map<Object, Long> remoteTopList) {

		DataPlacementRequestCommand command = commandsFactory
				.buildDataPlacementRequestCommand();
		command.init(dataPlacementManager, this.distributionManager);
		log.info("Putting Message with size " + remoteTopList.size());
		command.putRemoteList(remoteTopList, this.requestRound);
		if (!this.rpcManager.getAddress().toString().equals(owner)) {
			try {
				this.rpcManager.invokeRemotely(Collections.singleton(owner),
						command, false);
				log.info("Message sent to " + owner);
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
		
		log.info("Movedin Object list size :"+movedInList);
		log.info("Min Access Num is:"+minAccess);
		log.info("Number of objects moved in but not in local top list:"+tempList.size());
		for(Object key : tempList){
			localGetEstStatics.put(key, minAccess);
		}
		
		return localGetEstStatics;
	}
	
	
	private Map<Address, Map<Object, Long>> sortObjectsByOwner(
			Map<Object, Long> remoteGet) {
		Map<Address, Map<Object, Long>> objectLists = new HashMap<Address, Map<Object, Long>>();
		Map<Object, List<Address>> mappedObjects = getConsistentHashingFunction().locateAll(remoteGet.keySet(), 1);

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

		return objectLists;
	}
	
	public void postPhaseTest(){
//		log.info("Size of DataContainer: " + this.dataContainer.size());
//		log.info("Doing postphase testing! sentObjectList size:"
//				+ this.sentObjectList.size());
//		log.info("sentObjectsList: " + this.sentObjectList);
//		log.info("topremoteget: " + this.analyticsBean
//				.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
//						this.analyticsBean.getCapacity()));
//		//Check if there are some keys not moved out (that should be moved).
//		for (Entry<String, Pair<Integer,Integer>> entry : this.sentObjectList.entrySet()) {
//			if (this.dataContainer.containsKey(entry.getKey())) {
//				DataPlacementManager.log.error("postphase checking: Still contains key:"
//						+ entry.getKey());
//			}
//			else{
//				++entry.getValue().right;
//				sentObjectList.put(entry.getKey(),entry.getValue());
//			}
//		}
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
		
		log.info(requestSentList.toString());
	}
	
//	public boolean aggregateResult(Address sender,
//			Map<Object, Long> objectRequest, Integer roundID){
//		
//		try {
//			if (this.rpcManager.getTransport().getMembers().size() != this.addressList
//					.size()) {
//				this.addressList = this.rpcManager.getTransport().getMembers();
//				this.stateTransfer.setCachesList(this.addressList);
//			}
//
//			Integer senderID = this.addressList.indexOf(sender);
//			log.info("Getting message of round " + roundID + " from node"
//					+ sender);
//
//			if (roundID == this.replyRound) {
//				this.requestReceivedList.add(new Pair<Integer, Map<Object, Long>>(
//						senderID, objectRequest));
//			}
//
//			this.writer.write(false, sender, objectRequest);
//			
//			if (this.addressList.size() - this.requestReceivedList.size() == 1) {
//				return true;
//			}
//			else 
//			{
//				log.info("Gathering request... has received from"
//						+ this.requestReceivedList.size() + " nodes");
//				return false;
//			}
//		} catch (Exception e) {
//			log.error(e.toString());
//			return false;
//		}	
//	}
	
	public ConsistentHash getConsistentHashingFunction() {
	ConsistentHash hash = this.distributionManager.getConsistentHash();
	if (hash instanceof MachineLearningConsistentHashing)
		return ((MachineLearningConsistentHashing) hash).getDefaultHash();
	else
		return hash;
    }
}
