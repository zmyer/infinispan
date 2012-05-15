package org.infinispan.distribution.wrappers;

import org.apache.log4j.Logger;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.container.EntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.util.concurrent.locks.LockManager;
import org.rhq.helpers.pluginAnnotations.agent.Metric;

import java.lang.reflect.Field;

/**
 * Massive hack for a noble cause!
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
@MBean(objectName = "ExtendedStatistics", description = "Component that manages and exposes extended statistics relevant to transactions.")

public abstract class CustomStatsInterceptor extends BaseCustomInterceptor {

   private org.apache.log4j.Logger log = Logger.getLogger("org.infinispan.interceptors");




   @Start
   public void start(){
      replace();
      log.warn("Initing the TransactionStatisticsRegistry");
      TransactionsStatisticsRegistry.init();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      log.fatal("PutKeyValueCommand visited " + command);
      Object ret;
      if(ctx.isInTxScope()){
         this.initStatsIfNecessary(ctx);
         TransactionsStatisticsRegistry.setUpdateTransaction();
         long currTime = System.nanoTime();
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_PUTS);
         ret =  invokeNextInterceptor(ctx,command);
         if(isRemote(command.getKey())){
            TransactionsStatisticsRegistry.addValue(IspnStats.REMOTE_PUT_EXECUTION,System.nanoTime() - currTime);
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_REMOTE_PUT);
         }
         return ret;
      }
      else
         return invokeNextInterceptor(ctx,command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable{
      boolean isTx = ctx.isInTxScope();
      Object ret;
      if(isTx){
         this.initStatsIfNecessary(ctx);
         long currTime = 0;
         boolean isRemoteKey = isRemote(command.getKey());
         if(isRemoteKey && isTx){
            currTime = System.nanoTime();
         }

         ret = invokeNextInterceptor(ctx,command);
         if(isRemoteKey && isTx){
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_REMOTE_GET);
            TransactionsStatisticsRegistry.addValue(IspnStats.REMOTE_GET_EXECUTION, System.nanoTime() - currTime);
         }
      }
      else{
         ret = invokeNextInterceptor(ctx,command);
      }
      return ret;
   }

   protected boolean isRemote(Object key){
      return false;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      log.fatal("VISIT_COMMIT_COMMAND "+command);
      this.initStatsIfNecessary(ctx);
      long currTime = System.nanoTime();
      Object ret = invokeNextInterceptor(ctx,command);
      TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_COMMIT_COMMAND);
      TransactionsStatisticsRegistry.addValue(IspnStats.COMMIT_EXECUTION_TIME, System.nanoTime() - currTime);
      TransactionsStatisticsRegistry.terminateTransaction(true, ctx);
      return ret;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      log.fatal("Visiting PrepareCommand!! "+command);
      this.initStatsIfNecessary(ctx);
      TransactionsStatisticsRegistry.onPrepareCommand();

      return invokeNextInterceptor(ctx,command);
   }


   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable{
      this.initStatsIfNecessary(ctx);
      TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_ROLLBACKS);
      long initRollbackTime = System.nanoTime();
      Object ret = invokeNextInterceptor(ctx,command);
      TransactionsStatisticsRegistry.addValue(IspnStats.ROLLBACK_EXECUTION_TIME, System.nanoTime() - initRollbackTime);
      TransactionsStatisticsRegistry.terminateTransaction(false, ctx);
      return ret;
   }

   private void replace(){
      log.warn("CustomStatsInterceptor Enabled!");
      //System.out.println("CustomStatsInterceptor Enabled");
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();

      GlobalComponentRegistry globalComponentRegistry = componentRegistry.getGlobalComponentRegistry();
      InboundInvocationHandlerWrapper invocationHandlerWrapper = rewireInvocationHandler(globalComponentRegistry);
      globalComponentRegistry.rewire();

      replaceFieldInTransport(componentRegistry, invocationHandlerWrapper);

      replaceRpcManager(componentRegistry);
      replaceLockManager(componentRegistry);
      replaceEntryFactoryWrapper(componentRegistry);
      componentRegistry.rewire();

   }

   private void replaceFieldInTransport(ComponentRegistry componentRegistry, InboundInvocationHandlerWrapper invocationHandlerWrapper) {
      JGroupsTransport t = (JGroupsTransport) componentRegistry.getComponent(Transport.class);
      CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
      Field f = null;
      try {
         f = card.getClass().getDeclaredField("inboundInvocationHandler");
         f.setAccessible(true);
         f.set(card, invocationHandlerWrapper);
      } catch (NoSuchFieldException e) {
         e.printStackTrace();
      } catch (IllegalAccessException e) {
         e.printStackTrace();
      }
   }

   private InboundInvocationHandlerWrapper rewireInvocationHandler(GlobalComponentRegistry globalComponentRegistry) {
      InboundInvocationHandler inboundHandler = globalComponentRegistry.getComponent(InboundInvocationHandler.class);
      InboundInvocationHandlerWrapper invocationHandlerWrapper = new InboundInvocationHandlerWrapper(inboundHandler);
      globalComponentRegistry.registerComponent(invocationHandlerWrapper, InboundInvocationHandler.class);
      return invocationHandlerWrapper;
   }

   private void replaceEntryFactoryWrapper(ComponentRegistry componentRegistry) {
      EntryFactory entryFactory = componentRegistry.getComponent(EntryFactory.class);
      EntryFactoryWrapper entryFactoryWrapper = new EntryFactoryWrapper(entryFactory);
      componentRegistry.registerComponent(entryFactoryWrapper, EntryFactory.class);
   }

   private void replaceLockManager(ComponentRegistry componentRegistry) {
      LockManager lockManager = componentRegistry.getComponent(LockManager.class);
      LockManagerWrapper lockManagerWrapper = new LockManagerWrapper(lockManager);
      componentRegistry.registerComponent(lockManagerWrapper, LockManager.class);
   }

   private void replaceRpcManager(ComponentRegistry componentRegistry) {
      RpcManager rpcManager = componentRegistry.getComponent(RpcManager.class);
      RpcManagerWrapper rpcManagerWrapper = new RpcManagerWrapper(rpcManager);
      componentRegistry.registerComponent(rpcManagerWrapper, RpcManager.class);
   }


   private void initStatsIfNecessary(InvocationContext ctx){
      if(ctx.isInTxScope())
         TransactionsStatisticsRegistry.initTransactionIfNecessary((TxInvocationContext) ctx);
   }



   /*
   JMX exposed methods
    */

   @ManagedAttribute(description = "Number of puts")
   @Metric(displayName = "Average number of puts performed locally by a successful local transaction")
   public long getAvgNumPutsBySuccessfulLocalTx(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.PUTS_PER_LOCAL_TX);
   }

   @ManagedAttribute(description = "Average Rtt duration")
   @Metric(displayName = "Rtt")
   public long getRtt() {
      long ret = (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT)));
      log.warn("Sto ritornando con successo il valore "+ret);
      return ret;
   }

   @ManagedAttribute(description = "Application Contention Factor")
   @Metric(displayName = "ACF")
   public long getAcf() {
      return (Long)TransactionsStatisticsRegistry.getAttribute((IspnStats.APPLICATION_CONTENTION_FACTOR));
   }

   @ManagedAttribute(description = "Local Contention Probability")
   @Metric(displayName = "Local Conflict Probability")
   public double getLocalContentionProbability(){
      return (Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCAL_CONTENTION_PROBABILITY));
   }

   @ManagedAttribute(description = "Average time it takes to replicate successful modifications on the cohorts")
   @Metric(displayName = "Replay Time")
   public long getMaxReplayTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REPLAY_TIME) ;
   }



   //=====================  DEBUG!!! =============


   @ManagedAttribute(description = "LOCK_WAITING_TIME")
   @Metric(displayName = "LOCK_WAITING_TIME")
   public Object getLOCK_WAITING_TIME(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_WAITING_TIME);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "LOCK_HOLD_TIME")
   @Metric(displayName = "LOCK_HOLD_TIME")
   public Object getLOCK_HOLD_TIME(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_HELD_LOCKS")
   @Metric(displayName = "NUM_HELD_LOCKS")
   public Object getNUM_HELD_LOCKS(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_HELD_LOCKS);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "ROLLBACK_EXECUTION_TIME")
   @Metric(displayName = "ROLLBACK_EXECUTION_TIME")
   public Object getROLLBACK_EXECUTION_TIME(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.ROLLBACK_EXECUTION_TIME);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_ROLLBACKS")
   @Metric(displayName = "NUM_ROLLBACKS")
   public Object getNUM_ROLLBACKS(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_ROLLBACKS);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "WR_TX_LOCAL_EXECUTION_TIME")
   @Metric(displayName = "WR_TX_LOCAL_EXECUTION_TIME")
   public Object getWR_TX_LOCAL_EXECUTION_TIME(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_LOCAL_EXECUTION_TIME);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "RTT")
   @Metric(displayName = "RTT")
   public Object getRTT(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.RTT);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "REPLAY_TIME")
   @Metric(displayName = "REPLAY_TIME")
   public Object getREPLAY_TIME(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.REPLAY_TIME);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "REPLAYED_TXS")
   @Metric(displayName = "REPLAYED_TXS")
   public Object getREPLAYED_TXS(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.REPLAYED_TXS);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "PREPARE_COMMAND_SIZE")
   @Metric(displayName = "PREPARE_COMMAND_SIZE")
   public Object getPREPARE_COMMAND_SIZE(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.PREPARE_COMMAND_SIZE);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_COMMITTED_RO_TX")
   @Metric(displayName = "NUM_COMMITTED_RO_TX")
   public Object getNUM_COMMITTED_RO_TX(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_COMMITTED_RO_TX);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_COMMITTED_WR_TX")
   @Metric(displayName = "NUM_COMMITTED_WR_TX")
   public Object getNUM_COMMITTED_WR_TX(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_COMMITTED_WR_TX);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_ABORTED_WR_TX")
   @Metric(displayName = "NUM_ABORTED_WR_TX")
   public Object getNUM_ABORTED_WR_TX(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_ABORTED_WR_TX);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_ABORTED_RO_TX")
   @Metric(displayName = "NUM_ABORTED_RO_TX")
   public Object getNUM_ABORTED_RO_TX(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_ABORTED_RO_TX);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_SUCCESSFUL_RTTS")
   @Metric(displayName = "NUM_SUCCESSFUL_RTTS")
   public Object getNUM_SUCCESSFUL_RTTS(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_RTTS);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_PREPARES")
   @Metric(displayName = "NUM_PREPARES")
   public Object getNUM_PREPARES(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_PREPARES);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_PUTS")
   @Metric(displayName = "NUM_PUTS")
   public Object getNUM_PUTS(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_PUTS);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "COMMIT_EXECUTION_TIME")
   @Metric(displayName = "COMMIT_EXECUTION_TIME")
   public Object getCOMMIT_EXECUTION_TIME(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_EXECUTION_TIME);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "LOCAL_EXEC_NO_CONT")
   @Metric(displayName = "LOCAL_EXEC_NO_CONT")
   public Object getLOCAL_EXEC_NO_CONT(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_EXEC_NO_CONT);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "LOCAL_CONTENTION_PROBABILITY")
   @Metric(displayName = "LOCAL_CONTENTION_PROBABILITY")
   public Object getLOCAL_CONTENTION_PROBABILITY(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_CONTENTION_PROBABILITY);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "LOCK_CONTENTION_TO_LOCAL")
   @Metric(displayName = "LOCK_CONTENTION_TO_LOCAL")
   public Object getLOCK_CONTENTION_TO_LOCAL(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_CONTENTION_TO_LOCAL);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "LOCK_CONTENTION_TO_REMOTE")
   @Metric(displayName = "LOCK_CONTENTION_TO_REMOTE")
   public Object getLOCK_CONTENTION_TO_REMOTE(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_CONTENTION_TO_REMOTE);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_SUCCESSFUL_PUTS")
   @Metric(displayName = "NUM_SUCCESSFUL_PUTS")
   public Object getNUM_SUCCESSFUL_PUTS(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_PUTS);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "PUTS_PER_LOCAL_TX")
   @Metric(displayName = "PUTS_PER_LOCAL_TX")
   public Object getPUTS_PER_LOCAL_TX(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.PUTS_PER_LOCAL_TX);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_WAITED_FOR_LOCKS")
   @Metric(displayName = "NUM_WAITED_FOR_LOCKS")
   public Object getNUM_WAITED_FOR_LOCKS(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_WAITED_FOR_LOCKS);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_NODES_IN_PREPARE")
   @Metric(displayName = "NUM_NODES_IN_PREPARE")
   public Object getNUM_NODES_IN_PREPARE(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_NODES_IN_PREPARE);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_REMOTE_GET")
   @Metric(displayName = "NUM_REMOTE_GET")
   public Object getNUM_REMOTE_GET(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_REMOTE_GET);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "REMOTE_GET_EXECUTION")
   @Metric(displayName = "REMOTE_GET_EXECUTION")
   public Object getREMOTE_GET_EXECUTION(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_GET_EXECUTION);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "REMOTE_PUT_EXECUTION")
   @Metric(displayName = "REMOTE_PUT_EXECUTION")
   public Object getREMOTE_PUT_EXECUTION(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_PUT_EXECUTION);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_REMOTE_PUT")
   @Metric(displayName = "NUM_REMOTE_PUT")
   public Object getNUM_REMOTE_PUT(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_REMOTE_PUT);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "ARRIVAL_RATE")
   @Metric(displayName = "ARRIVAL_RATE")
   public Object getARRIVAL_RATE(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.ARRIVAL_RATE);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "TX_WRITE_PERCENTAGE")
   @Metric(displayName = "TX_WRITE_PERCENTAGE")
   public Object getTX_WRITE_PERCENTAGE(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_WRITE_PERCENTAGE);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "SUCCESSFUL_WRITE_PERCENTAGE")
   @Metric(displayName = "SUCCESSFUL_WRITE_PERCENTAGE")
   public Object getSUCCESSFUL_WRITE_PERCENTAGE(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.SUCCESSFUL_WRITE_PERCENTAGE);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "WR_TX_ABORTED_EXECUTION_TIME")
   @Metric(displayName = "WR_TX_ABORTED_EXECUTION_TIME")
   public Object getWR_TX_ABORTED_EXECUTION_TIME(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_ABORTED_EXECUTION_TIME);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "WR_TX_SUCCESSFUL_EXECUTION_TIME")
   @Metric(displayName = "WR_TX_SUCCESSFUL_EXECUTION_TIME")
   public Object getWR_TX_SUCCESSFUL_EXECUTION_TIME(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "RO_TX_SUCCESSFUL_EXECUTION_TIME")
   @Metric(displayName = "RO_TX_SUCCESSFUL_EXECUTION_TIME")
   public Object getRO_TX_SUCCESSFUL_EXECUTION_TIME(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "RO_TX_ABORTED_EXECUTION_TIME")
   @Metric(displayName = "RO_TX_ABORTED_EXECUTION_TIME")
   public Object getRO_TX_ABORTED_EXECUTION_TIME(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.RO_TX_ABORTED_EXECUTION_TIME);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "NUM_COMMIT_COMMAND")
   @Metric(displayName = "NUM_COMMIT_COMMAND")
   public Object getNUM_COMMIT_COMMAND(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_COMMIT_COMMAND);
      } catch(Exception e) {return null;}
   }
   @ManagedAttribute(description = "APPLICATION_CONTENTION_FACTOR")
   @Metric(displayName = "APPLICATION_CONTENTION_FACTOR")
   public Object getAPPLICATION_CONTENTION_FACTOR(){
      try {
         return TransactionsStatisticsRegistry.getAttribute(IspnStats.APPLICATION_CONTENTION_FACTOR);
      } catch(Exception e) {return null;}
   }



}
