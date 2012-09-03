package org.infinispan.dataplacement;

import org.infinispan.Cache;
import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.DataPlacementCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.dataplacement.lookup.ObjectLookupFactory;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.DistributedStateTransferManagerImpl;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Manages all phases in the dara placement protocol
 *
 * @author Zhongmiao Li
 * @author João Paiva
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "DataPlacementManager", description = "Manages the placement of the keys to support a better" +
      " performance in distributed mode")
@Listener
public class DataPlacementManager {

   private static final Log log = LogFactory.getLog(DataPlacementManager.class);

   private static final int INITIAL_COOL_DOWN_TIME = 30000; //30 seconds

   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private CacheViewsManager cacheViewsManager;

   private Boolean expectPre = true;
   private String cacheName;

   private RemoteAccessesManager remoteAccessesManager;
   private ObjectPlacementManager objectPlacementManager;
   private ObjectLookupManager objectLookupManager;

   private ObjectLookupFactory objectLookupFactory;

   private final RoundManager roundManager;

   public DataPlacementManager() {
      roundManager = new RoundManager(INITIAL_COOL_DOWN_TIME);
   }

   @Inject
   public void inject(CommandsFactory commandsFactory, DistributionManager distributionManager, RpcManager rpcManager,
                      CacheViewsManager cacheViewsManager, Cache cache, StateTransferManager stateTransfer,
                      DataContainer dataContainer, CacheNotifier cacheNotifier, CacheManagerNotifier cacheManagerNotifier,
                      Configuration configuration) {
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.cacheViewsManager = cacheViewsManager;
      this.cacheName = cache.getName();

      if (!configuration.dataPlacement().enabled()) {
         log.info("Data placement not enabled");
         return;
      }

      objectLookupFactory = configuration.dataPlacement().objectLookupFactory();
      objectLookupFactory.setConfiguration(configuration);

      roundManager.setCoolDownTime(configuration.dataPlacement().coolDownTime());

      //this is needed because the custom statistics invokes this method twice. the seconds time, it replaces
      //the original manager (== problems!!)
      synchronized (this) {
         if (stateTransfer instanceof DistributedStateTransferManagerImpl && !roundManager.isEnabled()) {
            remoteAccessesManager = new RemoteAccessesManager(distributionManager,dataContainer);
            objectPlacementManager = new ObjectPlacementManager(distributionManager,dataContainer);
            objectLookupManager = new ObjectLookupManager((DistributedStateTransferManagerImpl) stateTransfer);
            roundManager.enable();
            cacheNotifier.addListener(this);
            cacheManagerNotifier.addListener(this);
            log.info("Data placement enabled");
         } else {
            log.info("Data placement disabled. Not in Distributed mode");
         }
      }

   }

   @Start
   public void start() {
      updateMembersList(rpcManager.getTransport().getMembers());
   }

   /**
    * starts a new round of data placement protocol
    *
    * @param newRoundId the new round id
    */
   public final void startDataPlacement(long newRoundId) {
      if (log.isTraceEnabled()) {
         log.tracef("Start data placement protocol with round %s", newRoundId);
      }
      objectPlacementManager.resetState();
      objectLookupManager.resetState();
      remoteAccessesManager.resetState();
      roundManager.startNewRound(newRoundId);
      new Thread("Data-Placement-Thread") {
         @Override
         public void run() {
            try {
               sendRequestToAll();
            } catch (Exception e) {
               log.errorf(e, "Exception caught while starting data placement");
            }
         }
      }.start();
   }

   /**
    * collects all the request list from other members with the object that they want. when all requests are received
    * it decides to each member the object should go and it broadcast the Object Lookup
    *
    * @param sender        the sender
    * @param objectRequest the request list
    * @param roundId       the round id
    */
   public final void addRequest(Address sender, Map<Object, Long> objectRequest, long roundId) {
      if (log.isDebugEnabled()) {
         log.debugf("Keys request received from %s in round %s", sender, roundId);
      }

      if (!roundManager.ensure(roundId)) {
         log.warn("Not possible to process key request list");
         return;
      }

      if(objectPlacementManager.aggregateResult(sender, objectRequest)){
         Map<Object, Integer> objectsToMove = objectPlacementManager.getObjectsToMove();

         if (log.isTraceEnabled()) {
            log.tracef("All keys request list received. Object to move are " + objectsToMove);
         }

         ObjectLookup objectLookup = objectLookupFactory.createObjectLookup(objectsToMove);

         if (objectLookup != null) {
            objectPlacementManager.testObjectLookup(objectLookup);
         } else {
            log.warn("Object Lookup is null");
         }

         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.OBJECT_LOOKUP_PHASE,
                                                                                  roundManager.getCurrentRoundId());
         command.setObjectLookup(objectLookupFactory.serializeObjectLookup(objectLookup));

         rpcManager.broadcastRpcCommand(command, false, false);
         addObjectLookup(rpcManager.getAddress(), objectLookupFactory.serializeObjectLookup(objectLookup), roundId);
      }
   }

   /**
    * collects all the Object Lookup for each member. when all Object Lookup are collected, it sends an ack for the
    * coordinator
    *
    * @param from                   the originator
    * @param objectLookupParameters the serialize form of the Object Lookup
    * @param roundId                the round id
    */
   public final void addObjectLookup(Address from, Object[] objectLookupParameters, long roundId) {
      if (log.isDebugEnabled()) {
         log.debugf("Remote Object Lookup received from %s in round %s", from, roundId);
      }

      if (!roundManager.ensure(roundId)) {
         log.warn("Not possible to process remote Object Lookup");
         return;
      }

      ObjectLookup objectLookup = objectLookupFactory.deSerializeObjectLookup(objectLookupParameters);
      if (objectLookupManager.addObjectLookup(from, objectLookup)) {
         if (log.isTraceEnabled()) {
            log.tracef("All remote Object Lookup received. Send Ack to coordinator");
         }
         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.ACK_COORDINATOR_PHASE,
                                                                                  roundId);
         if (rpcManager.getTransport().isCoordinator()) {
            addAck(roundId);
         } else {
            rpcManager.invokeRemotely(Collections.singleton(rpcManager.getTransport().getCoordinator()), command, false);
         }
      }
   }

   /**
    * collects all acks from all members. when all acks are collects, the state transfer is triggered
    *
    * @param roundId the round id
    */
   public final void addAck(long roundId) {
      if (log.isDebugEnabled()) {
         log.debugf("Ack received in round %s", roundId);
      }

      if (!roundManager.ensure(roundId)) {
         log.warn("Not possible to process Ack");
         return;
      }

      if (objectLookupManager.addAck()) {
         if (log.isTraceEnabled()) {
            log.tracef("All Acks received. Trigger state transfer");
         }
         cacheViewsManager.handleRequestMoveKeys(cacheName);
      }
   }

   /**
    * sets the cool down time
    *
    * @param milliseconds  the new time in milliseconds
    */
   public final void internalSetCoolDownTime(int milliseconds) {
      roundManager.setCoolDownTime(milliseconds);
   }

   /**
    * obtains the request list to send for each member and sends it
    */
   private void sendRequestToAll() {
      if (log.isTraceEnabled()) {
         log.trace("Start sending keys request");
      }

      remoteAccessesManager.calculateRemoteAccessesPerMember();
      Map<Object, Long> request;

      for (Address address : rpcManager.getTransport().getMembers()) {
         request = remoteAccessesManager.getRemoteListFor(address);

         if (address.equals(rpcManager.getAddress())) {
            addRequest(address, request, roundManager.getCurrentRoundId());
         } else {
            DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.REMOTE_TOP_LIST_PHASE,
                                                                                     roundManager.getCurrentRoundId());
            command.setRemoteTopList(request);
            rpcManager.invokeRemotely(Collections.singleton(address), command, false);
            logRemoteTopListSent(request, address);
         }
      }
   }

   /**
    * log keys request list to owner
    *
    * @param request the request list
    * @param to      the owner
    */
   private void logRemoteTopListSent(Map<?,?> request, Address to) {
      if (log.isTraceEnabled()) {
         log.tracef("Sending request list of %s objects to %s. Request list is %s", request.size(), to, request);
      } else if (log.isDebugEnabled()) {
         log.debugf("Sending request list of %s objects to %s", request.size(), to);
      }
   }

   /**
    * updates the members in the cluster
    *
    * @param members the new members
    */
   private void updateMembersList(List<Address> members) {
      if (log.isDebugEnabled()) {
         log.debugf("Updating members list. New members are %s", members);
      }
      objectPlacementManager.updateMembersList(members);
      objectLookupManager.updateMembersList(members);
   }

   @Merged
   @ViewChanged
   public final void viewChange(ViewChangedEvent event) {
      if (log.isTraceEnabled()) {
         log.trace("View changed event trigger");
      }
      updateMembersList(event.getNewMembers());
   }

   @DataRehashed
   public final void keyMovementTest(DataRehashedEvent event) {
      if (log.isTraceEnabled()) {
         log.trace("Data rehashed event trigger");
      }
      if (event.getMembersAtEnd().size() == event.getMembersAtStart().size()) {
         if (event.isPre() && expectPre) {
            expectPre = false;
            if (log.isDebugEnabled()) {
               log.debug("Doing data placement pre-phase test");
               objectPlacementManager.prePhaseTest();
            }
         } else if(!event.isPre() && !expectPre) {
            expectPre = true;
            if (log.isDebugEnabled()) {
               log.debug("Doing data placement post-phase test");
               objectPlacementManager.postPhaseTest();
               remoteAccessesManager.postPhaseTest();
            }
            roundManager.markRoundFinished();
         }
      }
   }

   @ManagedOperation(description = "Start the data placement algorithm in order to optimize the system performance")
   public final void dataPlacementRequest() throws Exception {
      if (!rpcManager.getTransport().isCoordinator()) {
         if (log.isTraceEnabled()) {
            log.trace("Data placement request. Sending request to coordinator");
         }
         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.DATA_PLACEMENT_REQUEST,
                                                                                  roundManager.getCurrentRoundId());
         rpcManager.invokeRemotely(Collections.singleton(rpcManager.getTransport().getCoordinator()),
                                   command, false);
         return;
      }

      if (rpcManager.getTransport().getMembers().size() == 1) {
         log.warn("Data placement request received but we are the only member. ignoring...");
         return;
      }

      if (log.isTraceEnabled()) {
         log.trace("Data placement request received.");
      }

      DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.DATA_PLACEMENT_START,
                                                                               roundManager.getNewRoundId());
      rpcManager.broadcastRpcCommand(command, false, false);
      startDataPlacement(roundManager.getCurrentRoundId());
   }

   @ManagedOperation(description = "Updates the cool down time between two or more data placement requests")
   public final void setCoolDownTime(int milliseconds) {
      if (log.isTraceEnabled()) {
         log.tracef("Setting new cool down period to %s milliseconds", milliseconds);
      }
      DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.SET_COOL_DOWN_TIME,
                                                                               roundManager.getCurrentRoundId());
      command.setCoolDownTime(milliseconds);
      rpcManager.broadcastRpcCommand(command, false, false);
      internalSetCoolDownTime(milliseconds);
   }

   @ManagedAttribute(description = "The cache name", writable = false)
   public final String getCacheName() {
      return cacheName;
   }

   @ManagedAttribute(description = "The current cool down time between rounds", writable = false)
   public final long getCoolDownTime() {
      return roundManager.getCoolDownTime();
   }

   @ManagedAttribute(description = "The current round Id", writable = false)
   public final long getCurrentRoundId() {
      return roundManager.getCurrentRoundId();
   }

   @ManagedAttribute(description = "Check if a data placement round is in progress", writable = false)
   public final boolean isRoundInProgress() {
      return roundManager.isRoundInProgress();
   }

   @ManagedAttribute(description = "Check if the data placement is enabled", writable = false)
   public final boolean isEnabled() {
      return roundManager.isEnabled();
   }

   @ManagedAttribute(description = "The Object Lookup Factory class name", writable = false)
   public final String getObjectLookupFactoryClassName() {
      return objectLookupFactory.getClass().getCanonicalName();
   }
}
