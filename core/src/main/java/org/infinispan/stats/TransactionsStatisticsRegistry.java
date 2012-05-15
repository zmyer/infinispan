package org.infinispan.stats;

import org.apache.log4j.Logger;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.stats.translations.ExposedStatistics;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 */
public class TransactionsStatisticsRegistry {

   private static Logger log = Logger.getLogger(TransactionsStatisticsRegistry.class);

   //Now it is unbounded, we can define a MAX_NO_CLASSES
   private static HashMap<ExposedStatistics.TransactionalClasses, NodeScopeStatisticCollector> transactionalClassesStatsMap = new HashMap<ExposedStatistics.TransactionalClasses, NodeScopeStatisticCollector>();
   public static int LOCAL_PARAM = 0;
   public static int REMOTE_PARAM = 1;
   public static int GLOBAL_PARAM = 2;

   private static final ConcurrentMap<GlobalTransaction, RemoteTransactionStatistics> remoteTransactionStatistics =
         new ConcurrentHashMap<GlobalTransaction, RemoteTransactionStatistics>();

   public static void init(){
      transactionalClassesStatsMap.put(ExposedStatistics.TransactionalClasses.DEFAULT_CLASS, new NodeScopeStatisticCollector());
      log.debug("Initializing transactionalClassesMap");
   }

   /*
  Comment for reviewers: do we really need threadLocal? If I have the global id of the transaction, I can
  retrieve the transactionStatistics
   */
   private static final ThreadLocal<TransactionStatistics> thread = new ThreadLocal<TransactionStatistics>();


   public static void addValue(IspnStats param, double value) {
      TransactionStatistics txs = thread.get();
      txs.addValue(param, value);
   }

   public static void incrementValue(IspnStats param) {
      TransactionStatistics txs = thread.get();
      txs.addValue(param, 1D);
   }

   public static void onPrepareCommand() {
      //NB: If I want to give up using the InboundInvocationHandler, I can create the remote transaction
      //here, just overriding the handlePrepareCommand
      TransactionStatistics txs = thread.get();
      txs.onPrepareCommand();
   }

   public static void setTransactionOutcome(boolean commit) {
      TransactionStatistics txs = thread.get();
      txs.setCommit(commit);
   }

   public static void terminateTransaction() {
      log.fatal("TERMINATING_TRANSACTION");
      TransactionStatistics txs = thread.get();
      txs.terminateTransaction();

      NodeScopeStatisticCollector dest = transactionalClassesStatsMap.get(txs.getTransactionalClass());
      dest.merge(txs);

      thread.remove();
   }

   public static Object getAttribute(ExposedStatistics.IspnStats param){
      log.warn("Going to invoke getAttribute with parameter "+param);
      return transactionalClassesStatsMap.get(ExposedStatistics.TransactionalClasses.DEFAULT_CLASS).getAttribute(param);
   }

   public static void addTakenLock(Object lock) {
      TransactionStatistics txs = thread.get();
      txs.addTakenLock(lock);
   }


   public static void setUpdateTransaction() {
      TransactionStatistics txs = thread.get();
      txs.setUpdateTransaction();
   }

   //This is synchronized because depending on the local/remote nature, a different object is created
   //Now, remote transactionStatistics get initialized at InboundInvocationHandler level
   public static void initTransactionIfNecessary(TxInvocationContext tctx) {
      boolean isLocal = tctx.isOriginLocal();
      if(isLocal)
         initLocalTransaction();

   }

   private static void initLocalTransaction(){
      //Not overriding the InitialValue method leads me to have "null" at the first invocation of get()
      if (thread.get() == null) {
         log.fatal("THREAD.GET==NULL!!!!");
         thread.set(new LocalTransactionStatistics());
      } else {
         log.fatal("THREAD.GET!=NULL!!!!");
      }
   }

   public static void attachRemoteTransactionStatistic(GlobalTransaction globalTransaction) {
      RemoteTransactionStatistics rts = remoteTransactionStatistics.get(globalTransaction);
      if (rts == null) {
         rts = new RemoteTransactionStatistics();
         remoteTransactionStatistics.put(globalTransaction, rts);
      }
      thread.set(rts);
   }

   public static void detachRemoteTransactionStatistic(GlobalTransaction globalTransaction, boolean finished) {
      if (finished) {
         terminateTransaction();
         remoteTransactionStatistics.remove(globalTransaction);
      } else {
         thread.remove();
      }
   }

   public static void reset(){
      for(ExposedStatistics.TransactionalClasses n: transactionalClassesStatsMap.keySet()){
         transactionalClassesStatsMap.get(n).reset();
      }
   }


}
