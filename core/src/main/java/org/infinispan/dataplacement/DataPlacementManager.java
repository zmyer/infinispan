package org.infinispan.dataplacement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.infinispan.Cache;
import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.dataplacement.DataPlacementReplyCommand;
import org.infinispan.commands.dataplacement.DataPlacementReplyCommand.DATAPLACEPHASE;
import org.infinispan.commands.dataplacement.DataPlacementRequestCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.dataplacement.c50.TreeElement;
import org.infinispan.dataplacement.lookup.BloomFilter;
import org.infinispan.dataplacement.lookup.ObjectLookUpper;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.DistributedStateTransferManagerImpl;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


@Listener
public class DataPlacementManager {

	private static final Log log = LogFactory
			.getLog(DataPlacementManager.class);
	private RpcManager rpcManager;
	private DistributionManager distributionManager;
	private CommandsFactory commandsFactory;
	private CacheViewsManager cacheViewsManager;
	private DistributedStateTransferManagerImpl stateTransfer;
	private DataContainer dataContainer;

	private String cacheName;
	private Timer popuTimer, dataPlaceTimer;
	private int preDataContainerSize;
	
	private StreamLibContainer analyticsBean;
	private TestWriter writer = new TestWriter();

	private Object lookUpperLock = new Object(), ackLock = new Object();

    //Round  counter
	private Integer requestRound = 0, replyRound = 0, lookUpperNumber = 0,
			hasAckedNumber = 0;
	private Boolean expectPre = true;

	private List<Address> addressList;
	//Key of pair is destination node, value of pair is moved number 
	private Map<String, Pair<Integer, Integer>> sentObjectList = new HashMap<String, Pair<Integer, Integer>>();
	private List<Pair<Integer, Map<Object, Long>>> requestReceivedList = new ArrayList<Pair<Integer, Map<Object, Long>>>();
	private Set<Object> requestSentList = new HashSet<Object>();
	private List<Object> movedInList = new ArrayList<Object>();

	public DataPlacementManager() {
	}

	@Inject
	public void inject(CommandsFactory commandsFactory,
			DistributionManager distributionManager, RpcManager rpcManager,
			CacheViewsManager cacheViewsManager, Cache cache,
			StateTransferManager stateTransfer, DataContainer dataContainer,
			CacheNotifier cacheNotifier) {
		this.commandsFactory = commandsFactory;
		this.distributionManager = distributionManager;
		this.rpcManager = rpcManager;
		this.cacheViewsManager = cacheViewsManager;
		this.cacheName = cache.getName();
		this.dataContainer = dataContainer;
		if (stateTransfer instanceof DistributedStateTransferManagerImpl) 
		  this.stateTransfer = (DistributedStateTransferManagerImpl) stateTransfer;
		this.analyticsBean = StreamLibContainer.getInstance();
		this.addressList = new ArrayList<Address>();
		cacheNotifier.addListener(this);
	}

	@Start
	public void startTimer() {
		popuTimer = new Timer();
		popuTimer.schedule(new WaitingPopulationTask(), 30000, 10000);
	}

	public void sendRequestToAll() {
		log.info("Start sending requests");
		Map<Object, Long> remoteGet = this.analyticsBean
				.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
						this.analyticsBean.getCapacity());
		
		// remotePut =
		// analyticsBean.getTopKFrom(Anal	yticsBean.Stat.REMOTE_PUT,analyticsBean.getCapacity());

		// Only send statistics if there are enough objects
		if (remoteGet.size() >= this.analyticsBean.getCapacity() * 0.8) {
			
			Map<Object, Long> localGet = getStasticsForMovedInObj();
			log.info("Moved In Object Size:"+localGet.size());
			remoteGet.putAll(localGet);
			log.info("Merged List Size:"+remoteGet.size());
			Map<Address, Map<Object, Long>> remoteTopLists = this.sortObjectsByOwner(remoteGet);
			requestSentList = remoteGet.keySet();
			
			log.info("Size of list is ");

			for (Entry<Address, Map<Object, Long>> entry : remoteTopLists
					.entrySet()) {
				this.sendRequest(entry.getKey(), entry.getValue());
			}
			List<Address> addresses = this.rpcManager.getTransport().getMembers();
			for (Address add : addresses) {
				if (remoteTopLists.containsKey(add) == false
						&& add.equals(this.cacheName)) {
					this.sendRequest(add, new HashMap<Object, Long>());
				}
			}
			++this.requestRound;
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
		
		for(Object key : tempList){
			localGetEstStatics.put(key, minAccess);
		}
		
		return localGetEstStatics;
	}


	private void sendRequest(Address owner, Map<Object, Long> remoteTopList) {

		DataPlacementRequestCommand command = this.commandsFactory
				.buildDataPlacementRequestCommand();
		command.init(this, this.distributionManager);
		log.info("Putting Message with size " + remoteTopList.size());
		command.putRemoteList(remoteTopList, this.requestRound);
		if (!this.rpcManager.getAddress().toString().equals(owner)) {
			try {
				this.rpcManager.invokeRemotely(Collections.singleton(owner),
						command, false);
				DataPlacementManager.log.info("Message sent to " + owner);
				this.writer.write(true, null, remoteTopList);
			} catch (Throwable throwable) {
				DataPlacementManager.log.error(throwable.toString());
			}
		} else {
			DataPlacementManager.log.warn("Message will not be sent to myself!");
		}
	}

	private Map<Address, Map<Object, Long>> sortObjectsByOwner(
			Map<Object, Long> remoteGet) {
		Map<Address, Map<Object, Long>> objectLists = new HashMap<Address, Map<Object, Long>>();
		Map<Object, List<Address>> mappedObjects = this.distributionManager
				.locateAll(remoteGet.keySet(), 1);

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

	// Aggregate!
	public void aggregateRequests(Address sender,
			Map<Object, Long> objectRequest, Integer roundID) {
		DataPlacementManager.log.info("Aggregating request!");
		try {
			if (this.rpcManager.getTransport().getMembers().size() != this.addressList
					.size()) {
				this.addressList = this.rpcManager.getTransport().getMembers();
				this.stateTransfer.setCachesList(this.addressList);
			}

			Integer senderID = this.addressList.indexOf(sender);
			DataPlacementManager.log.info("Getting message of round " + roundID + " from node"
					+ sender);

			if (roundID == this.replyRound) {
				this.requestReceivedList.add(new Pair<Integer, Map<Object, Long>>(
						senderID, objectRequest));
			}

			if (this.addressList.size() - this.requestReceivedList.size() == 1) {
				this.writer.write(false, sender, objectRequest);
				sendReplyToAll();
				
			} else {
				DataPlacementManager.log.info("Gathering request... has received from"
						+ this.requestReceivedList.size() + " nodes");
			}
		} catch (Exception e) {
			DataPlacementManager.log.error(e.toString());
		}
	}
	
	public void sendReplyToAll(){
		
		log.info("Everyone has sent request!!! "
				+ this.addressList.size()
				+ " in total!");
		Map<Object, Pair<Long, Integer>> fullRequestList = compactRequestList();
		List<Pair<String, Integer>> finalResultList = generateFinalList(fullRequestList);
		log.info("Writing result");
		writer.writeResult(finalResultList);

		for(Pair<String, Integer> pair : finalResultList){
		  sentObjectList.put(pair.left, new Pair<Integer, Integer>(pair.right,0));		  	
		}
		
		log.info("Populate All");

		ObjectLookUpper lookUpper = new ObjectLookUpper(finalResultList);

		DataPlacementManager.log.info("Rules:");
		DataPlacementManager.log.info(lookUpper.printRules());
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
		DataPlacementManager.log.info("Merged:" + mergeCnt);
		DataPlacementManager.log.info("Conflict but succeeded:" + conflictSuccCnt);
		DataPlacementManager.log.info("Conflict but failed:" + conflictFailCnt);
		DataPlacementManager.log.info("Size of fullrequestList: " + fullRequestList.size());

		return fullRequestList;
	}

	/*
	 * Compare the remote access of every entry in the full request list and
	 * return the final resultList
	 */
	public List<Pair<String, Integer>> generateFinalList(
			Map<Object, Pair<Long, Integer>> fullRequestList) {
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
		DataPlacementManager.log.info("Succeeded conflict in final :" + succeededConflict);
		DataPlacementManager.log.info("Failed conflict in final :" + failedConflict);

		return resultList;
	}

	public void sendLookUpper(BloomFilter simpleBloomFilter,
			List<List<TreeElement>> treeList) {
		DataPlacementReplyCommand command = this.commandsFactory
				.buildDataPlacementReplyCommand();
		command.init(this);
		command.setPhase(DATAPLACEPHASE.SETTING_PHASE);
		command.putBloomFilter(simpleBloomFilter);
		command.putTreeElement(treeList);

		this.buildMLHashAndAck(this.rpcManager.getAddress(), simpleBloomFilter, treeList);

		try {
			this.rpcManager.invokeRemotely(null, command, false);
		} catch (Throwable throwable) {
			DataPlacementManager.log.error(throwable.toString());
		}
	}



	public void buildMLHashAndAck(Address address, BloomFilter bf,
			List<List<TreeElement>> treeList) {
		synchronized (this.lookUpperLock) {
			this.stateTransfer.setLookUpper(address, new ObjectLookUpper(bf,
					treeList));
			++this.lookUpperNumber;
		}
		DataPlacementManager.log.info("Look Upper Set: " + this.lookUpperNumber);
		synchronized (this.lookUpperLock) {
			if (this.lookUpperNumber == this.addressList.size()) {
				this.lookUpperNumber = 0;
				this.sendAck(this.rpcManager.getTransport().getCoordinator());
			}
		}
	}
	
	public void sendAck(Address coordinator) {
		DataPlacementReplyCommand command = this.commandsFactory
				.buildDataPlacementReplyCommand();
		command.init(this);
		command.setPhase(DATAPLACEPHASE.ACK_PHASE);
		DataPlacementManager.log.info("Sending Ack to Coordinator: " + coordinator);
		try {
			this.rpcManager.invokeRemotely(Collections.singleton(coordinator),
					command, false);
		} catch (Throwable throwable) {
			DataPlacementManager.log.error(throwable.toString());
		}
	}

	public void aggregateAck() {
		synchronized (this.ackLock) {
			++this.hasAckedNumber;
			DataPlacementManager.log.info("Has aggregated Ack :" + this.hasAckedNumber);
			if (this.hasAckedNumber == this.rpcManager.getTransport().getMembers().size() - 1) {
				DataPlacementManager.log.info("Start moving keys.");
				this.hasAckedNumber = 0;
				String s = "";
				this.cacheViewsManager.handleRequestMoveKeys(this.cacheName);
			}
		}
	}

	@DataRehashed
	public void keyMovementTest(DataRehashedEvent event) {
		if (event.getMembersAtEnd().size() == event.getMembersAtStart().size()) {
			DataPlacementManager.log.info("Doing Keymovement test!");
			if (event.isPre() && expectPre) {
				expectPre = false;
				log.info("View ID:"+event.getNewViewId());
				log.info("Size of DataContainer: " + dataContainer.size());
				log.info("Doing prephase testing! sentObjectList size:"
						+ sentObjectList.size());
				//log.info("sentObjectsList: " + sentObjectList);
				log.info("topremoteget: " + analyticsBean.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
								this.analyticsBean.getCapacity()));
				for (String key : this.sentObjectList.keySet()) {
					if (!this.dataContainer.containsKey(key)) {
						DataPlacementManager.log.error("prephase checking: Does't contains key:"
								+ key);
					}
				}
			} else if( !event.isPre() && !expectPre ){
				expectPre = true;
				log
						.info("Size of DataContainer: " + this.dataContainer.size());
				log.info("Doing postphase testing! sentObjectList size:"
						+ this.sentObjectList.size());
				log.info("sentObjectsList: " + this.sentObjectList);
				log.info("topremoteget: " + this.analyticsBean
						.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
								this.analyticsBean.getCapacity()));
				//Check if there are some keys not moved out (that should be moved).
				for (Entry<String, Pair<Integer,Integer>> entry : this.sentObjectList.entrySet()) {
					if (this.dataContainer.containsKey(entry.getKey())) {
						DataPlacementManager.log.error("postphase checking: Still contains key:"
								+ entry.getKey());
					}
					else{
						++entry.getValue().right;
						sentObjectList.put(entry.getKey(),entry.getValue());
					}
				}
				log.info("Sent List Size: "+sentObjectList.size());
				
				log.info("Adding moved-in list");
				
				//Add moved-in keys into the list
				for (Object key : requestSentList){
					if(dataContainer.containsKey(key)){
						movedInList.add(key);
					}
					analyticsBean.resetAll();
				}
				
				log.info(requestSentList.toString());
			}
		} else {
			DataPlacementManager.log.info("KeyMovementTest not triggered!");
		}
	}

	class WaitingPopulationTask extends TimerTask {
		@Override
		public void run() {
			if(dataContainer.size() == preDataContainerSize){
				log.info("Start data placment request!");
				dataPlaceTimer = new Timer();
				dataPlaceTimer.schedule(new DataPlaceRequestTask(), 120000, 120000);
				popuTimer.cancel();
			}
			else{
				preDataContainerSize = dataContainer.size();
				log.info("Still running popultion!");
			}
		}
	}
	
	class DataPlaceRequestTask extends TimerTask {
		@Override
		public void run() {
			DataPlacementManager.this.sendRequestToAll();
			DataPlacementManager.log.info("Timer Runned Once!");
		}
	}
}
