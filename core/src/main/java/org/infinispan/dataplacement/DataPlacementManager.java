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
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.DistributedStateTransferManagerImpl;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
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
                      DataContainer dataContainer, CacheNotifier cacheNotifier, Configuration configuration) {
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.cacheViewsManager = cacheViewsManager;
      this.cacheName = cache.getName();

      if (!configuration.dataPlacement().enabled()) {
         return;
      }

      objectLookupFactory = configuration.dataPlacement().objectLookupFactory();
      objectLookupFactory.setConfiguration(configuration);

      roundManager.setCoolDownTime(configuration.dataPlacement().coolDownTime());

      if (stateTransfer instanceof DistributedStateTransferManagerImpl) {
         remoteAccessesManager = new RemoteAccessesManager(distributionManager,dataContainer);
         objectPlacementManager = new ObjectPlacementManager(distributionManager,dataContainer);
         objectLookupManager = new ObjectLookupManager((DistributedStateTransferManagerImpl) stateTransfer);
         roundManager.enable();
      }

      cacheNotifier.addListener(this);
      log.info("My cache name is "+ cache.getName());
   }

   /**
    * starts a new round of data placement protocol
    *
    * @param newRoundId the new round id
    */
   public final void startDataPlacement(long newRoundId) {
      objectPlacementManager.resetState();
      objectLookupManager.resetState();
      remoteAccessesManager.resetState();
      roundManager.startNewRound(newRoundId);
      new Thread() {
         @Override
         public void run() {
            sendRequestToAll();
         }
      };
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
      try {
         roundManager.ensure(roundId);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         log.warnf("The thread was interrupted while waiting for the round %s. returning...", roundId);
         return;
      } catch (Exception e) {
         log.errorf("Data placement not enabled");
         return;
      }

      DataPlacementManager.log.info("Aggregating request!");
      if(objectPlacementManager.aggregateResult(sender, objectRequest)){
         Map<Object, Integer> objectsToMove = objectPlacementManager.getObjectsToMove();
         ObjectLookup objectLookup = objectLookupFactory.createObjectLookup(objectsToMove);
         if (objectLookup == null) {
            objectPlacementManager.testObjectLookup(objectLookup);
         }

         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.OBJECT_LOOKUP_PHASE,
                                                                                  roundManager.getCurrentRoundId());
         command.setObjectLookup(objectLookupFactory.serializeObjectLookup(objectLookup));

         rpcManager.broadcastRpcCommand(command, false, false);
         objectLookupManager.addObjectLookup(rpcManager.getAddress(), objectLookup);
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
      try {
         roundManager.ensure(roundId);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         log.warnf("The thread was interrupted while waiting for the round %s. returning...", roundId);
         return;
      } catch (Exception e) {
         log.errorf("Data placement not enabled");
         return;
      }
      ObjectLookup objectLookup = objectLookupFactory.deSerializeObjectLookup(objectLookupParameters);
      if (objectLookupManager.addObjectLookup(from, objectLookup)) {
         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.ACK_COORDINATOR_PHASE,
                                                                                  roundId);
         rpcManager.invokeRemotely(Collections.singleton(rpcManager.getTransport().getCoordinator()), command, false);
      }
   }

   /**
    * collects all acks from all members. when all acks are collects, the state transfer is triggered
    *
    * @param roundId the round id
    */
   public final void addAck(long roundId) {
      try {
         roundManager.ensure(roundId);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         log.warnf("The thread was interrupted while waiting for the round %s. returning...", roundId);
         return;
      } catch (Exception e) {
         log.errorf("Data placement not enabled");
         return;
      }
      if (objectLookupManager.addAck()) {
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

   @ViewChanged
   public void viewChange(ViewChangedEvent event) {
      objectPlacementManager.updateMembersList(event.getNewMembers());
      objectLookupManager.updateMembersList(event.getNewMembers());
   }

   @DataRehashed
   public void keyMovementTest(DataRehashedEvent event) {
      if (event.getMembersAtEnd().size() == event.getMembersAtStart().size()) {
         DataPlacementManager.log.info("Doing Keymovement test!");
         if (event.isPre() && expectPre) {
            expectPre = false;
            log.info("View ID:"+event.getNewViewId());
            objectPlacementManager.prePhaseTest();
         } else if( !event.isPre() && !expectPre ){
            expectPre = true;
            objectPlacementManager.postPhaseTest();
            remoteAccessesManager.postPhaseTest();
            roundManager.markRoundFinished();
         }
      } else {
         DataPlacementManager.log.info("KeyMovementTest not triggered!");
      }
   }

   /**
    * obtains the request list to send for each member and sends it
    */
   private void sendRequestToAll() {
      log.info("Start sending requests");

      remoteAccessesManager.calculateRemoteAccessesPerMember();
      Map<Object, Long> request;

      for (Address address : rpcManager.getTransport().getMembers()) {
         request = remoteAccessesManager.getRemoteListFor(address);
         if (request == null) {
            request = Collections.emptyMap();
         }
         if (address.equals(rpcManager.getAddress())) {
            addRequest(address, request, roundManager.getCurrentRoundId());
         } else {
            DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.REMOTE_TOP_LIST_PHASE,
                                                                                     roundManager.getCurrentRoundId());
            command.setRemoteTopList(request);
            rpcManager.invokeRemotely(Collections.singleton(address), command, false);
         }
      }
   }

   @ManagedOperation(description = "Start the data placement algorithm in order to optimize the system performance")
   public final void dataPlacementRequest() throws Exception {
      if (!rpcManager.getTransport().isCoordinator()) {
         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.DATA_PLACEMENT_REQUEST,
                                                                                  roundManager.getCurrentRoundId());
         rpcManager.invokeRemotely(Collections.singleton(rpcManager.getTransport().getCoordinator()),
                                   command, false);
         return;
      }
      DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.DATA_PLACEMENT_START,
                                                                               roundManager.getNewRoundId());
      rpcManager.broadcastRpcCommand(command, false, false);
      startDataPlacement(roundManager.getCurrentRoundId());
   }

   @ManagedOperation(description = "Updates the cool down time between two or more data placement requests")
   public final void setCoolDownTime(int milliseconds) {
      DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.SET_COOL_DOWN_TIME,
                                                                               roundManager.getCurrentRoundId());
      command.setCoolDownTime(milliseconds);
      rpcManager.broadcastRpcCommand(command, false, false);
      internalSetCoolDownTime(milliseconds);
   }
}
