package org.infinispan.dataplacement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;

import org.infinispan.Cache;
import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.dataplacement.DataPlacementReplyCommand;
import org.infinispan.commands.dataplacement.DataPlacementReplyCommand.DATAPLACEPHASE;
import org.infinispan.commands.dataplacement.DataPlacementRequestCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.dataplacement.c50.TreeElement;
import org.infinispan.dataplacement.lookup.ObjectLookUpper;
import org.infinispan.dataplacement.lookup.SimpleBloomFilter;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.BaseStateTransferManagerImpl;
import org.infinispan.statetransfer.DistributedStateTransferManagerImpl;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.clearspring.analytics.util.Pair;

@Listener
public class DataPlacementManager {

	private static final Log log = LogFactory
			.getLog(DataPlacementManager.class);
	private RpcManager rpcManager;
	private DistributionManager distributionManager;
	private CommandsFactory commandsFactory;

	private Timer timer;
	private StreamLibContainer analyticsBean;
	private ObjectLookUpper lookUpper = new ObjectLookUpper();

	private TestWriter writer = new TestWriter();
	private CacheViewsManager cacheViewsManager;
	private Cache cache;
	private DistributedStateTransferManagerImpl stateTransfer;

	private Object lookUpperLock = new Object(), ackLock = new Object();
	private Address myAddress;
	private DataContainer dataContainer;

	private Integer requestRound = 0, replyRound = 0, lookUpperNumber = 0,
			hasAckedNumber = 0;

	private List<Address> addressList;
	private List<Pair<Integer, Map<Object, Long>>> objectRequestList = new ArrayList<Pair<Integer, Map<Object, Long>>>();
	private List<Pair<String, Integer>> sentObjectList;
	private CacheNotifier cacheNotifier;

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
		this.cache = cache;
		this.dataContainer = dataContainer;
		if (stateTransfer instanceof DistributedStateTransferManagerImpl) {
			log.info("Is Dist");
		} else
			log.info("Is not Dist");
		this.stateTransfer = (DistributedStateTransferManagerImpl) stateTransfer;
		myAddress = rpcManager.getAddress();
		this.analyticsBean = StreamLibContainer.getInstance();
		this.addressList = new ArrayList<Address>();
		cacheNotifier.addListener(this);
		this.cacheNotifier = cacheNotifier;
	}

	@Start
	public void startTimer() {
		timer = new Timer();
		timer.schedule(new DataPlaceRequestTask(), 120000, 600000);
	}

	public void sendRequestToAll() {
		Map<Object, Long> remoteGet = analyticsBean
				.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
						analyticsBean.getCapacity());
		// remotePut =
		// analyticsBean.getTopKFrom(AnalyticsBean.Stat.REMOTE_PUT,analyticsBean.getCapacity());

		// Only send statistics if there are enough objects
		if (remoteGet.size() >= 800) {
			Map<Address, Map<Object, Long>> remoteTopLists = sortObjectsByOwner(remoteGet);

			for (Entry<Address, Map<Object, Long>> entry : remoteTopLists
					.entrySet()) {
				sendRequest(entry.getKey(), entry.getValue());
			}
			List<Address> addresses = rpcManager.getTransport().getMembers();
			for (Address add : addresses) {
				if (remoteTopLists.containsKey(add) == false
						&& add.equals(cache.getName()))
					sendRequest(add, new HashMap<Object, Long>());
			}

			++requestRound;
		}
	}

	private void sendRequest(Address owner, Map<Object, Long> remoteTopList) {

		DataPlacementRequestCommand command = commandsFactory
				.buildDataPlacementRequestCommand();
		command.init(this, distributionManager);
		log.info("Putting Message with size " + remoteTopList.size());
		command.putRemoteList(remoteTopList, requestRound);
		if (!rpcManager.getAddress().toString().equals(owner)) {
			try {
				rpcManager.invokeRemotely(Collections.singleton(owner),
						command, false);
				log.info("Message sent to " + owner);
				writer.write(true, null, remoteTopList);
			} catch (Throwable throwable) {
				log.error(throwable);
			}
		} else {
			log.warn("Message will not be sent to myself!");
		}
	}

	private Map<Address, Map<Object, Long>> sortObjectsByOwner(
			Map<Object, Long> remoteGet) {
		Map<Address, Map<Object, Long>> objectLists = new HashMap<Address, Map<Object, Long>>();
		Map<Object, List<Address>> mappedObjects = distributionManager
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
		log.info("Aggregating request!");
		try {
			// log.info(rpcManager.getTransport().getMembers());
			// log.info(addressList);
			if (rpcManager.getTransport().getMembers().size() != addressList
					.size()) {
				// log.info("Before Add");
				addressList = rpcManager.getTransport().getMembers();
				// log.info("Before StateTransfer :" +stateTransfer);
				stateTransfer.setCachesList(addressList);
			}
			// log.info("before get index of add");
			Integer senderID = addressList.indexOf(sender);
			log.info("Getting message of round " + roundID + " from node"
					+ sender);

			if (roundID == replyRound)
				objectRequestList.add(new Pair<Integer, Map<Object, Long>>(
						senderID, objectRequest));

			if (addressList.size() - objectRequestList.size() == 1) {
				log.info("Everyone has sent request!!! " + addressList.size()
						+ " in total!");
				writer.write(false, sender, objectRequest);

				Map<Object, Pair<Long, Integer>> fullRequestList = compactRequestList();
				List<Pair<String, Integer>> finalResultList = generateFinalList(fullRequestList);
				writer.writeResult(finalResultList);

				sentObjectList = finalResultList;
				lookUpper.populateAll(finalResultList);

				log.info("Rules:");
				log.info(lookUpper.getRules());
				sendLookUpper(lookUpper.getBloomFilter(),
						lookUpper.getTreeList());
				objectRequestList.clear();
				++replyRound;
			} else
				log.info("Gathering request... has received from"
						+ objectRequestList.size() + " nodes");
		} catch (Exception e) {
			log.error(e.toString());
		}
	}

	/*
	 * Merge the request lists from all other nodes into a single request list
	 */
	public Map<Object, Pair<Long, Integer>> compactRequestList() {
		Map<Object, Pair<Long, Integer>> fullRequestList = new HashMap<Object, Pair<Long, Integer>>();

		Map<Object, Long> requestList = objectRequestList.get(0).right;
		Integer addressIndex = objectRequestList.get(0).left;

		// Put objects of the first lisk into the fullList
		for (Entry<Object, Long> entry : requestList.entrySet()) {
			fullRequestList.put(entry.getKey(),
					new Pair<Long, Integer>(entry.getValue(), addressIndex));
		}

		// For the following lists, when merging into the full list, has to
		// compare if its request has the
		// highest remote access
		int conflictFailCnt = 0, conflictSuccCnt = 0, mergeCnt = 0;
		for (int i = 1; i < objectRequestList.size(); ++i) {
			requestList = objectRequestList.get(i).right;
			addressIndex = objectRequestList.get(i).left;
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
		List<Pair<String, Integer>> resultList = new ArrayList<Pair<String, Integer>>();
		Map<Object, Long> localGetList = analyticsBean.getTopKFrom(
				StreamLibContainer.Stat.LOCAL_GET, analyticsBean.getCapacity());
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
			} else
				++failedConflict;
		}
		log.info("Succeeded conflict in final :" + succeededConflict);
		log.info("Failed conflict in final :" + failedConflict);

		return resultList;
	}

	public void sendLookUpper(SimpleBloomFilter simpleBloomFilter,
			List<List<TreeElement>> treeList) {
		DataPlacementReplyCommand command = commandsFactory
				.buildDataPlacementReplyCommand();
		command.init(this);
		command.setPhase(DATAPLACEPHASE.SETTING_PHASE);
		command.putBloomFilter(simpleBloomFilter);
		command.putTreeElement(treeList);

		setLookUpper(rpcManager.getAddress(), simpleBloomFilter, treeList);

		try {
			rpcManager.invokeRemotely(null, command, false);
		} catch (Throwable throwable) {
			log.error(throwable);
		}
	}

	public void sendAck(Address coordinator) {
		DataPlacementReplyCommand command = commandsFactory
				.buildDataPlacementReplyCommand();
		command.init(this);
		command.setPhase(DATAPLACEPHASE.ACK_PHASE);
		log.info("Sending Ack to Coordinator: " + coordinator);
		try {
			rpcManager.invokeRemotely(Collections.singleton(coordinator),
					command, false);
		} catch (Throwable throwable) {
			log.error(throwable);
		}
	}

	public void setLookUpper(Address address, SimpleBloomFilter bf,
			List<List<TreeElement>> treeList) {
		synchronized (lookUpperLock) {
			stateTransfer.setLookUpper(address, new ObjectLookUpper(bf,
					treeList));
			++lookUpperNumber;
		}
		log.info("Look Upper Set: " + lookUpperNumber);
		synchronized (lookUpperLock) {
			if (lookUpperNumber == addressList.size()) {
				lookUpperNumber = 0;
				sendAck(rpcManager.getTransport().getCoordinator());
			}
		}
	}

	public void aggregateAck() {
		synchronized (ackLock) {
			++hasAckedNumber;
			log.info("Has aggregated Ack :" + hasAckedNumber);
			if (hasAckedNumber == rpcManager.getTransport().getMembers().size() - 1) {
				log.info("Start moving keys.");
				hasAckedNumber = 0;
				cacheViewsManager.handleRequestMoveKeys(cache.getName());
			}
		}
	}

	@DataRehashed
	public void keyMovementTest(DataRehashedEvent event) {
		if (event.getMembersAtEnd().size() == event.getMembersAtStart().size()) {
			log.info("Doing Keymovement test!");
			if (event.isPre()) {
				log.info("Size of DataContainer: " + dataContainer.size());
				log.info("Doing prephase testing! sentObjectList size:"
						+ sentObjectList.size());
				log.info("sentObjectsList: " + sentObjectList);
				log.info("dataContainer: " + dataContainer.keySet());
				log.info("topremoteget: " + analyticsBean
						.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
								analyticsBean.getCapacity()) );
				for (Pair<String, Integer> pair : sentObjectList) {
					if (!dataContainer.containsKey(pair.left)) {
						log.error("PRE PHASE VALIDATING: Does't contains key:"
								+ pair.left);
					}
				}
			} else {
				log.info("Size of DataContainer: " + dataContainer.size());
				log.info("Doing postphase testing! sentObjectList size:"
						+ sentObjectList.size());
				log.info("sentObjectsList: " + sentObjectList);
				log.info("topremoteget: " + analyticsBean
						.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,
								analyticsBean.getCapacity()) );
				for (Pair<String, Integer> pair : sentObjectList) {
					if (dataContainer.containsKey(pair.left)) {
						log.error("POST PHASE VALIDATING: Still contains key:"
								+ pair.left);
					}
				}
			}
		} else
			log.info("keyMovementTest not triggered!");
	}

	class DataPlaceRequestTask extends TimerTask {
		public void run() {
			sendRequestToAll();
			log.info("Timer Runned Once!");
		}
	}
}
