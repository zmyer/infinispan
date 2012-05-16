package org.infinispan.distribution.wrappers;

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
import org.infinispan.factories.annotations.Inject;
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
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.Metric;

import java.lang.reflect.Field;

/**
 * Massive hack for a noble cause!
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "ExtendedStatistics", description = "Component that manages and exposes extended statistics " +
      "relevant to transactions.")
public abstract class CustomStatsInterceptor extends BaseCustomInterceptor {
   //TODO what about the transaction implicit vs transaction explicit? should we take in account this and ignore
   //the implicit stuff?

   private final Log log = LogFactory.getLog(getClass());

   private TransactionTable transactionTable;

   @Inject
   public void inject(TransactionTable transactionTable) {
      this.transactionTable = transactionTable;
   }

   @Start
   public void start(){
      replace();
      log.warn("Initializing the TransactionStatisticsRegistry");
      TransactionsStatisticsRegistry.init();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      log.tracef("Visit Put Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                 ctx.isInTxScope(), ctx.isOriginLocal());
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
      log.tracef("Visit Get Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                 ctx.isInTxScope(), ctx.isOriginLocal());
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
      log.tracef("Visit Commit command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction());
      this.initStatsIfNecessary(ctx);
      long currTime = System.nanoTime();
      Object ret = invokeNextInterceptor(ctx,command);
      TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_COMMIT_COMMAND);
      TransactionsStatisticsRegistry.addValue(IspnStats.COMMIT_EXECUTION_TIME, System.nanoTime() - currTime);
      TransactionsStatisticsRegistry.setTransactionOutcome(true);
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction();
      }
      return ret;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      log.tracef("Visit Prepare command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction());
      this.initStatsIfNecessary(ctx);
      TransactionsStatisticsRegistry.onPrepareCommand();
      if (command.hasModifications()) {
         TransactionsStatisticsRegistry.setUpdateTransaction();
      }

      return invokeNextInterceptor(ctx,command);
   }


   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable{
      log.tracef("Visit Rollback command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction());
      this.initStatsIfNecessary(ctx);
      TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_ROLLBACKS);
      long initRollbackTime = System.nanoTime();
      Object ret = invokeNextInterceptor(ctx,command);
      TransactionsStatisticsRegistry.addValue(IspnStats.ROLLBACK_EXECUTION_TIME, System.nanoTime() - initRollbackTime);
      TransactionsStatisticsRegistry.setTransactionOutcome(false);
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction();
      }
      return ret;
   }

   private void replace(){
      log.infof("CustomStatsInterceptor Enabled!");
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
      try {
         Field f = card.getClass().getDeclaredField("inboundInvocationHandler");
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
      InboundInvocationHandlerWrapper invocationHandlerWrapper = new InboundInvocationHandlerWrapper(inboundHandler,
                                                                                                     transactionTable);
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

   //JMX exposed methods

   @ManagedAttribute(description = "Number of puts")
   @Metric(displayName = "Average number of puts performed locally by a successful local transaction")
   public long getAvgNumPutsBySuccessfulLocalTx(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.PUTS_PER_LOCAL_TX);
   }

   @ManagedAttribute(description = "Average Rtt duration")
   @Metric(displayName = "Rtt")
   public long getRtt() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT)));
   }

   @ManagedAttribute(description = "Application Contention Factor")
   @Metric(displayName = "ACF")
   public double getAcf() {
      return (Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.APPLICATION_CONTENTION_FACTOR));
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

   @ManagedAttribute(description = "Local execution time of a transaction without the time waiting for lock acquisition")
   @Metric(displayName = "Local Execution Time Without Locking Time")
   public long getLocalExecutionTimeWithoutLock(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_EXEC_NO_CONT);
   }

   @ManagedAttribute(description = "Average lock holding time")
   @Metric(displayName = "Average Lock Holding Time")
   public long getAvgLockHoldTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME);
   }

   @ManagedAttribute(description = "Average commit duration time (2nd phase only)")
   @Metric(displayName = "Average Commit Time")
   public long getAvgCommitTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average rollback duration time (2nd phase only)")
   @Metric(displayName = "Average Rollback Time")
   public long getAvgRollbackTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.ROLLBACK_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average Prepare Command size")
   @Metric(displayName = "Average Prepare Command size")
   public long getAvgPrepareCommandSize(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.PREPARE_COMMAND_SIZE);
   }

   @ManagedAttribute(description = "Average time waiting for the lock acquisition")
   @Metric(displayName = "Average Lock Waiting Time")
   public long getAvgLockWaitingTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_WAITING_TIME);
   }

   @ManagedAttribute(description = "Average transaction arrival rate")
   @Metric(displayName = "Average Transaction Arrival Rate")
   public long getAvgTxArrivalRate(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.ARRIVAL_RATE);
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed locally (committed and aborted)")
   @Metric(displayName = "Percentage of Write Transactions")
   public double getPercentageWriteTransactions(){
      return (Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_WRITE_PERCENTAGE);
   }

   @ManagedAttribute(description = "Percentage of successfully Read-Write transaction executed locally")
   @Metric(displayName = "Percentage of Successfully Write Transactions")
   public double getPercentageSuccessWriteTransactions(){
      return (Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.SUCCESSFUL_WRITE_PERCENTAGE);
   }
}
