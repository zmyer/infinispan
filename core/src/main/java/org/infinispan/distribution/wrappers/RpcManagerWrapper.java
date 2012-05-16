package org.infinispan.distribution.wrappers;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Map;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class RpcManagerWrapper implements RpcManager {
   private static final Log log = LogFactory.getLog(RpcManagerWrapper.class);
   private final RpcManager actual;

   //TODO split the rtt for commit, prepare and remote get
   public RpcManagerWrapper(RpcManager actual) {
      this.actual = actual;
   }

   //TODO The remote gets are invoked by this guy
   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, boolean usePriorityQueue,
                                                ResponseFilter responseFilter, boolean totalOrder){
      log.tracef("RpcManagerWrapper.invokeRemotely");
      long currentTime = System.nanoTime();

      Map<Address,Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue,
                                                        responseFilter, totalOrder);
      if (rpcCommand instanceof ClusteredGetCommand && TransactionsStatisticsRegistry.hasStatisticCollector()) {
         TransactionsStatisticsRegistry.addValue(IspnStats.RTT,System.nanoTime() - currentTime);
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_SUCCESSFUL_RTTS);
         TransactionsStatisticsRegistry.addValue(IspnStats.NUM_NODES_IN_PREPARE,recipients.size());
      }
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, 
                                                ResponseMode mode, long timeout, boolean usePriorityQueue, boolean totalOrder) {      
      log.tracef("RpcManagerWrapper.invokeRemotely_1");
      return actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, totalOrder);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout) {
      log.tracef("RpcManagerWrapper.invokeRemotely_2");
      return actual.invokeRemotely(recipients, rpcCommand, mode, timeout);
   }   

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean totalOrder) throws RpcException {
      System.out.println("RpcManagerWrapper.broadcastRpcCommand");
      actual.broadcastRpcCommand(rpc, sync, totalOrder);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      System.out.println("DIE : broadcastRpcCommand");
      actual.broadcastRpcCommand(rpc, sync, usePriorityQueue, totalOrder);
   }      

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      log.tracef("RpcManagerWrapper.broadcastRpcCommandInFuture");
      actual.broadcastRpcCommandInFuture(rpc, future);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue,
                                           NotifyingNotifiableFuture<Object> future) {
      log.tracef("RpcManagerWrapper.broadcastRpcCommandInFuture_1");
      actual.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
   }

   @Override
   public void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws RpcException {
      log.tracef("RpcManagerWrapper.invokeRemotely _3");
      actual.invokeRemotely(recipients, rpc, sync);
   }      

   //TODO The commit/rollback are invoked by this guy
   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, 
                                                boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      log.tracef("RpcManagerWrapper.invokeRemotely_4");
      Map<Address,Response> ret;
      long currentTime = System.nanoTime();
      ret = actual.invokeRemotely(recipients, rpc, sync, usePriorityQueue, totalOrder);
      if (shouldCollectStats(rpc)) {
         TransactionsStatisticsRegistry.addValue(IspnStats.RTT,System.nanoTime() - currentTime);
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_SUCCESSFUL_RTTS);
         TransactionsStatisticsRegistry.addValue(IspnStats.NUM_NODES_IN_PREPARE, recipients == null ?
               actual.getTransport().getMembers().size() :
               recipients.size());
      }
      return ret;
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc,
                                      NotifyingNotifiableFuture<Object> future) {
      log.tracef("RpcManagerWrapper.invokeRemotelyInFuture");
      actual.invokeRemotelyInFuture(recipients, rpc, future);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future) {
      log.tracef("RpcManagerWrapper.invokeRemotelyInFuture_1");
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout) {
      log.tracef("RpcManagerWrapper.invokeRemotelyInFuture_2");
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      log.tracef("RpcManagerWrapper.invokeRemotelyInFuture_3");
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout, ignoreLeavers);
   }

   @Override
   public Transport getTransport() {
      log.tracef("RpcManagerWrapper.getTransport");
      return actual.getTransport();
   }

   @Override
   public Address getAddress() {
      log.tracef("RpcManagerWrapper.getAddress");
      return actual.getAddress();
   }

   private boolean shouldCollectStats(ReplicableCommand cacheRpcCommand) {
      return cacheRpcCommand instanceof PrepareCommand || cacheRpcCommand instanceof CommitCommand;
   }
}
