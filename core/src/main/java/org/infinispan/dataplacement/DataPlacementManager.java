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
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.MachineLearningConsistentHashing;
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

	private DataContainer dataContainer;
	private RpcManager rpcManager;
	private Cache cache;
	private Timer popuTimer, dataPlaceTimer;
	private int preDataContainerSize;
	
	private int DATAPLACE_INTERVAL = 160000, 
			    POPULATION_CHECK_INTERVAL = 10000;
	
	private Boolean expectPre = true;	
	private RequestManager requestManager;
	private ReplyManager replyManager;
	

	public DataPlacementManager() {
	}

	@Inject
	public void inject(CommandsFactory commandsFactory,
			DistributionManager distributionManager, RpcManager rpcManager,
			CacheViewsManager cacheViewsManager, Cache cache,
			StateTransferManager stateTransfer, DataContainer dataContainer,
			CacheNotifier cacheNotifier) {
		this.dataContainer = dataContainer;
		this.cache = cache;
		this.rpcManager = rpcManager;
		if (stateTransfer instanceof DistributedStateTransferManagerImpl) 
		{
		requestManager = new RequestManager(commandsFactory, rpcManager, distributionManager, 
				(DistributedStateTransferManagerImpl)stateTransfer, dataContainer, cache.getName(), this);
		replyManager = new ReplyManager(commandsFactory, cacheViewsManager, distributionManager, (DistributedStateTransferManagerImpl)stateTransfer, 
				rpcManager, dataContainer,cache.getName(), this);
		}
		cacheNotifier.addListener(this);
		log.info("My cache name is "+ cache.getName());
	}

	@Start
	public void startTimer() {
		popuTimer = new Timer();
		popuTimer.schedule(new WaitingPopulationTask(), 
				POPULATION_CHECK_INTERVAL, POPULATION_CHECK_INTERVAL);
	}

	public void sendRequestToAll() {
		log.info("Start sending requests");
		
		Map<Object, Long> requestSelf = requestManager.sendRequestToAll();
		
		if(requestSelf!=null){
		    aggregateRequests(rpcManager.getAddress(), requestSelf, requestManager.getRoundID());
		    requestManager.increaseRoundID();
		}
		else
			log.info("Request not ready (top key not filled)!");
		
	}


	// Aggregate!
	public void aggregateRequests(Address sender,
			Map<Object, Long> objectRequest, Integer roundID) {
		DataPlacementManager.log.info("Aggregating request!");
		if( replyManager.aggregateResult(sender, objectRequest, roundID)){
			replyManager.sendReplyToAll();
		}
	}

	public void buildMLHashAndAck(Address address, BloomFilter bf,
			List<List<TreeElement>> treeList) {
		replyManager.buildMLHashAndAck(address, bf, treeList);
	}
	
	public void aggregateAck() {
		replyManager.aggregateAck();
	}

	@DataRehashed
	public void keyMovementTest(DataRehashedEvent event) {
		if (event.getMembersAtEnd().size() == event.getMembersAtStart().size()) {
			DataPlacementManager.log.info("Doing Keymovement test!");
			if (event.isPre() && expectPre) {
				expectPre = false;
				log.info("View ID:"+event.getNewViewId());
				replyManager.prePhaseTest();
			} else if( !event.isPre() && !expectPre ){
				expectPre = true;
				replyManager.postPhaseTest();
				requestManager.postPhaseTest();
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
				dataPlaceTimer.schedule(new DataPlaceRequestTask(), DATAPLACE_INTERVAL, DATAPLACE_INTERVAL);
				popuTimer.cancel();
			}
			else{
				preDataContainerSize = dataContainer.size();
				log.info("Still populating!");
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
