package org.infinispan.distribution.wrappers;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

import java.util.Collection;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
public class RpcManagerWrapper implements RpcManager {
   private final RpcManager actual;

   public RpcManagerWrapper(RpcManager actual) {
      this.actual = actual;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
      System.out.println("RpcManagerWrapper.invokeRemotely");
      return actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
      System.out.println("RpcManagerWrapper.invokeRemotely");
      return actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) {
      System.out.println("RpcManagerWrapper.invokeRemotely");
      return actual.invokeRemotely(recipients, rpcCommand, mode, timeout);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws RpcException {
      System.out.println("RpcManagerWrapper.broadcastRpcCommand");
      actual.broadcastRpcCommand(rpc, sync);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      System.out.println("RpcManagerWrapper.broadcastRpcCommand");
      actual.broadcastRpcCommand(rpc, sync, usePriorityQueue);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      System.out.println("RpcManagerWrapper.broadcastRpcCommandInFuture");
      actual.broadcastRpcCommandInFuture(rpc, future);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      System.out.println("RpcManagerWrapper.broadcastRpcCommandInFuture");
      actual.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
   }

   @Override
   public void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws RpcException {
      System.out.println("RpcManagerWrapper.invokeRemotely");
      actual.invokeRemotely(recipients, rpc, sync);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      System.out.println("RpcManagerWrapper.invokeRemotely");
      return actual.invokeRemotely(recipients, rpc, sync, usePriorityQueue);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      System.out.println("RpcManagerWrapper.invokeRemotelyInFuture");
      actual.invokeRemotelyInFuture(recipients, rpc, future);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      System.out.println("RpcManagerWrapper.invokeRemotelyInFuture");
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout) {
      System.out.println("RpcManagerWrapper.invokeRemotelyInFuture");
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      System.out.println("RpcManagerWrapper.invokeRemotelyInFuture");
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout, ignoreLeavers);
   }

   @Override
   public Transport getTransport() {
      System.out.println("RpcManagerWrapper.getTransport");
      return actual.getTransport();
   }

   @Override
   public Address getAddress() {
      System.out.println("RpcManagerWrapper.getAddress");
      return actual.getAddress();
   }
}
