package org.infinispan.dataplacement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.dataplacement.DataPlacementReplyCommand;
import org.infinispan.commands.dataplacement.DataPlacementReplyCommand.DATAPLACEPHASE;
import org.infinispan.container.DataContainer;
import org.infinispan.dataplacement.c50.TreeElement;
import org.infinispan.dataplacement.lookup.BloomFilter;
import org.infinispan.dataplacement.lookup.ObjectLookUpper;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.DistributedStateTransferManagerImpl;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class ReplyManager {
	

	private static final Log log = LogFactory.getLog(ReplyManager.class);
	private Object lookUpperLock = new Object(), ackLock = new Object();
	private StreamLibContainer analyticsBean;
	private DistributedStateTransferManagerImpl stateTransfer;

	private List<Address> addressList = new ArrayList<Address>();
	private CommandsFactory commandsFactory;
	private RpcManager rpcManager;
	private List<Pair<String, Integer>> currentRoundFinalObjects;
	private Map<String, Pair<Integer, Integer>> allSentObjects = new HashMap<String, Pair<Integer, Integer>>();
	private List<Pair<Integer, Map<Object, Long>>> requestReceivedList = new ArrayList<Pair<Integer, Map<Object, Long>>>();

	private Integer requestRound = 0,replyRound = 0;
	private Integer  lookUpperNumber = 0,
			hasAckedNumber = 0;
	private CacheViewsManager cacheViewsManager;
	private DataContainer dataContainer;
	private String cacheName;
	private TestWriter writer = TestWriter.getInstance();
	private DataPlacementManager dataPlacementManager;
	private DistributionManager distributionManager;
	
	public ReplyManager(CommandsFactory commandsFactory,CacheViewsManager cacheViewsManager,DistributionManager distributionManager, DistributedStateTransferManagerImpl  stateTransfer,
			RpcManager rpcManager, DataContainer dataContainer, String cacheName, DataPlacementManager dataPlacementManager){
		analyticsBean = StreamLibContainer.getInstance();
		this.commandsFactory = commandsFactory; 
		this.stateTransfer = stateTransfer;
		this.cacheViewsManager = cacheViewsManager;
		this.rpcManager = rpcManager;
		this.dataContainer = dataContainer;
		this.dataPlacementManager = dataPlacementManager;
		this.cacheName = cacheName;
		this.distributionManager = distributionManager;
	}
	
	public boolean aggregateResult(Address sender,
			Map<Object, Long> objectRequest, Integer roundID){
		
		try {
			if (this.rpcManager.getTransport().getMembers().size() != this.addressList
					.size()) {
				this.addressList = this.rpcManager.getTransport().getMembers();
				this.stateTransfer.setCachesList(this.addressList);
			}

			Integer senderID = this.addressList.indexOf(sender);
			log.info("Getting message of round " + roundID + " from node"
					+ sender);

			if (roundID == this.replyRound) {
				this.requestReceivedList.add(new Pair<Integer, Map<Object, Long>>(
						senderID, objectRequest));
			}

			writer.getInstance().write(false, sender, objectRequest);
			
			if (this.addressList.size() == this.requestReceivedList.size()) {
				return true;
			}
			else 
			{
				log.info("Gathering request... has received from"
						+ this.requestReceivedList.size() + " nodes");
				return false;
			}
		} catch (Exception e) {
			log.error(e.toString());
			return false;
		}	
	}
		
    public void sendReplyToAll(){
		this.addressList = this.rpcManager.getTransport().getMembers();
		
		log.info("Everyone has sent request!!! "
				+ this.addressList.size()
				+ " in total!");
		Map<Object, Pair<Long, Integer>> fullRequestList = compactRequestList();
		currentRoundFinalObjects = generateFinalList(fullRequestList);
		log.info("Number of current round sending objects "+ currentRoundFinalObjects.size());
		//writer.writeResult(currentRoundList);

//		for(Pair<String, Integer> pair : currentRoundSentObjects){
//			Pair<Integer,Integer> temp = sentObjects.get(pair.left);
//		    if(temp == null)
//			 sentObjects.put(pair.left, new Pair<Integer, Integer>(pair.right,1));		  	
//		    else
//		      ++temp.right;
//		    // log.warn("Try to move object twice!");
//		}
		
		
		//log.info("Sent Objects:"+allSentObjects.size());
		log.info("Populate All");

		ObjectLookUpper lookUpper = new ObjectLookUpper(currentRoundFinalObjects);

		log.info("Rules:");
		log.info(lookUpper.printRules());
		
		log.warn("Testing bloom filter before sending!");
		int bfErrorCount = 0;
		for(Pair<String, Integer> pair: currentRoundFinalObjects){
			if(lookUpper.query(pair.left) != pair.right){
				++bfErrorCount;
			}
		}
		log.warn("Error of BF before sending :"+bfErrorCount);
		
		this.sendLookUpper(lookUpper.getBloomFilter(),
				lookUpper.getTreeList());
		this.requestReceivedList.clear();
		++this.replyRound;
	}
    
	/*
	 * Merge the request lists from all other nodes into a single request list
	 */
	public Map<Object, Pair<Long, Integer>> compactRequestList() {
		Map<Object, Pair<Long, Integer>> fullRequestList = new HashMap<Object, Pair<Long, Integer>>();

		Map<Object, Long> requestList = this.requestReceivedList.get(0).right;
		Integer addressIndex = this.requestReceivedList.get(0).left;

		// Put objects of the first lisk into the fullList
		for (Entry<Object, Long> entry : requestList.entrySet()) {
			fullRequestList.put(entry.getKey(),
					new Pair<Long, Integer>(entry.getValue(), addressIndex));
		}

		// For the following lists, when merging into the full list, has to
		// compare if its request has the highest remote access
		int conflictFailCnt = 0, conflictSuccCnt = 0, mergeCnt = 0;
		for (int i = 1; i < this.requestReceivedList.size(); ++i) {
			requestList = this.requestReceivedList.get(i).right;
			addressIndex = this.requestReceivedList.get(i).left;
			for (Entry<Object, Long> entry : requestList.entrySet()) {
				Pair<Long, Integer> pair = fullRequestList.get(entry.getKey());
				if (pair == null) {
					fullRequestList.put(entry.getKey(),
							new Pair<Long, Integer>(entry.getValue(),
									addressIndex));
					++mergeCnt;
				} else {
					if (pair.left < entry.getValue()) {
						fullRequestList.put(entry.getKey(),
								new Pair<Long, Integer>(entry.getValue(),
										addressIndex));
						++conflictSuccCnt;
					} else {
						++conflictFailCnt;
					}
				}
			}
		}
		log.info("Merged:" + mergeCnt);
		log.info("Conflict but succeeded:" + conflictSuccCnt);
		log.info("Conflict but failed:" + conflictFailCnt);
		log.info("Size of fullrequestList: " + fullRequestList.size());

		return fullRequestList;
	}
	
	/*
	 * Compare the remote access of every entry in the full request list and
	 * return the final resultList
	 */
	public List<Pair<String, Integer>> generateFinalList(
			Map<Object, Pair<Long, Integer>> fullRequestList) {
		log.info("Generating final list");
		List<Pair<String, Integer>> resultList = new ArrayList<Pair<String, Integer>>();
		Map<Object, Long> localGetList = this.analyticsBean.getTopKFrom(
				StreamLibContainer.Stat.LOCAL_GET, this.analyticsBean.getCapacity());
		// localPutList =
		// analyticsBean.getTopKFrom(AnalyticsBean.Stat.LOCAL_PUT,
		// analyticsBean.getCapacity());

		// !TODO Has to modify back for better efficiency
		int failedConflict = 0, succeededConflict = 0;
		for (Entry<Object, Pair<Long, Integer>> entry : fullRequestList
				.entrySet()) {
			if (localGetList.containsKey(entry.getKey()) == false) {
				resultList.add(new Pair<String, Integer>(entry.getKey()
						.toString(), entry.getValue().right));
			} else if (localGetList.get(entry.getKey()) < entry.getValue().left) {
				resultList.add(new Pair<String, Integer>(entry.getKey()
						.toString(), entry.getValue().right));
				++succeededConflict;
			} else {
				++failedConflict;
			}
		}
		log.info("Succeeded conflict in final :" + succeededConflict);
		log.info("Failed conflict in final :" + failedConflict);

		return resultList;
	}
	
	public void sendLookUpper(BloomFilter simpleBloomFilter,
			List<List<TreeElement>> treeList) {
		log.info("Sending LookUpper");
		DataPlacementReplyCommand command = this.commandsFactory
				.buildDataPlacementReplyCommand();
		command.init(dataPlacementManager);
		command.setPhase(DATAPLACEPHASE.SETTING_PHASE);
		command.putBloomFilter(simpleBloomFilter);
		command.putTreeElement(treeList);

		log.info("Building ML Hash");
		this.buildMLHashAndAck(this.rpcManager.getAddress(), simpleBloomFilter, treeList);

		try {
			this.rpcManager.invokeRemotely(null, command, false);
		} catch (Throwable throwable) {
			log.error(throwable.toString());
		}
	}

	public void buildMLHashAndAck(Address address, BloomFilter bf,
			List<List<TreeElement>> treeList) {
		synchronized (this.lookUpperLock) {
			this.stateTransfer.setLookUpper(address, new ObjectLookUpper(bf,
					treeList));
			++this.lookUpperNumber;
		//}
		log.info("Look Upper Set: " + this.lookUpperNumber);
		//synchronized (this.lookUpperLock) {
			if (this.lookUpperNumber == this.addressList.size()) {
				this.lookUpperNumber = 0;
				this.sendAck(this.rpcManager.getTransport().getCoordinator());
			}
		}
	}
	
	public void sendAck(Address coordinator) {
		DataPlacementReplyCommand command = this.commandsFactory
				.buildDataPlacementReplyCommand();
		command.init(dataPlacementManager);
		command.setPhase(DATAPLACEPHASE.ACK_PHASE);
		log.info("Sending Ack to Coordinator: " + coordinator);
		try {
			this.rpcManager.invokeRemotely(Collections.singleton(coordinator),
					command, false);
		} catch (Throwable throwable) {
			log.error(throwable.toString());
		}
	}

	public void aggregateAck() {
		synchronized (this.ackLock) {
			++this.hasAckedNumber;
			log.info("Has aggregated Ack :" + this.hasAckedNumber);
			if (this.hasAckedNumber == this.rpcManager.getTransport().getMembers().size() - 1) {
				log.info("Start moving keys.");
				this.hasAckedNumber = 0;
				String s = "";
				this.cacheViewsManager.handleRequestMoveKeys(this.cacheName);
			}
		}
	}
	
	public void prePhaseTest(){
		//log.info("DataContainer: " + dataContainer.entrySet());
		log.info("Doing prephase testing! sentObjectList size:"
				+ allSentObjects.size());
		log.info("Current Round Sent Object size:"
				+ currentRoundFinalObjects.size());
		//log.info("sentObjectsList: " + sentObjectList);
		log.info("topremoteget: " + analyticsBean.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
						this.analyticsBean.getCapacity()));
		for (Pair<String,Integer> pair : currentRoundFinalObjects) {
			if (allSentObjects.containsKey(pair.left) == false &&!this.dataContainer.containsKey(pair.left)) {
				log.error("prephase checking: Doesn't contains key:"
						+ pair.left);
			}
		}
	}
	
	public void postPhaseTest(){
		log.info("Doing postphase testing!");
		log.info("Size of DataContainer: " + this.dataContainer.size());
		log.info("SentObjectsList size before merging current round: " + this.allSentObjects.size());
		log.info("topremoteget: " + this.analyticsBean
				.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
						this.analyticsBean.getCapacity()));
	
		
		//Check if try to move some key twice
		for(Pair<String, Integer> pair : currentRoundFinalObjects){
			Pair<Integer,Integer> temp = allSentObjects.get(pair.left);
		    if(temp == null)
			 allSentObjects.put(pair.left, new Pair<Integer, Integer>(pair.right,1));		  	
		    else if(pair.right !=  temp.left){
		      ++temp.right;
		      log.warn("Try to move object twice!");
		    }
		}
		log.info("SentObjectsList size after merging current round: " + this.allSentObjects.size());
		
		
		int stillContainsCount = 0;
		//Check if there are some keys not moved out (that should be moved).
		for (Entry<String, Pair<Integer,Integer>> entry : this.allSentObjects.entrySet()) {
			if (this.dataContainer.containsKey(entry.getKey())) {
				++stillContainsCount;
				//log.error("postphase checking: Still contains key:"
				//		+ entry.getKey());
			}		
		//	else{
		//		++entry.getValue().right;
				//sentObjects.put(entry.getKey(),entry.getValue());
		//	}
		}
		
		log.info(stillContainsCount+ " keys are not moved out correctly!");
		
		stillContainsCount = 0;
		for (Pair<String,Integer> entry : currentRoundFinalObjects) {
			if (this.dataContainer.containsKey(entry.left)) {
				++stillContainsCount;
			}		
		}
		log.info("Testing with currentRoundList: "+stillContainsCount+ " keys are not moved out correctly!");
		
		log.warn("Testing hash look up after sending!");
		int bfErrorCount = 0;
		for(Pair<String, Integer> pair: currentRoundFinalObjects){
			if(distributionManager.getConsistentHash().locate(pair.left,1).get(0).equals(addressList.get(pair.right)) == false){
				++bfErrorCount;
			}
		}
		
		
		log.warn("Error of hash look up after sending :"+bfErrorCount);
		
//		log.info("Request List Size: "+requestSentList.size());
//		log.info("Movedin List Size: "+ movedInList.size());
//		
//		log.info("Adding moved-in list");
//		
//		//Add moved-in keys into the list
//		for (Object key : requestSentList){	
//			if(dataContainer.containsKey(key)){
//				movedInList.add(key);
//			}
//			analyticsBean.resetAll();
//		}
//		
//		log.info(requestSentList.toString());
	}
}
