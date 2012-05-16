package org.infinispan.stats;

import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.stats.translations.ExposedStatistics.TransactionalClasses;
import org.infinispan.stats.translations.LocalRemoteStatistics;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;


/**
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class TransactionStatistics implements InfinispanStat {

   //Here the elements which are common for local and remote transactions
   protected long initTime;
   private boolean isReadOnly;
   private boolean isCommit;
   private TransactionalClasses transactionalClass;
   private HashMap<Object, Long> takenLocks = new HashMap<Object, Long>();
   protected final static int NON_COMMON_STAT = -1;

   private final StatisticsContainer statisticsContainer;

   private final Log log = LogFactory.getLog(getClass());


   public TransactionStatistics(int size) {
      this.initTime = System.nanoTime();
      this.isReadOnly = true; //as far as it does not tries to perform a put operation
      this.takenLocks = new HashMap<Object, Long>();
      this.transactionalClass = TransactionalClasses.DEFAULT_CLASS;
      this.statisticsContainer = new StatisticsContainerImpl(size);
      log.tracef("Created transaction statistics. Class is %s. Start time is %s",
                 transactionalClass, initTime);
   }

   public final TransactionalClasses getTransactionalClass(){
      return this.transactionalClass;
   }

   public final boolean isCommit(){
      return this.isCommit;
   }

   public final void setCommit(boolean commit) {
      isCommit = commit;
   }

   public final boolean isReadOnly(){
      return this.isReadOnly;
   }

   public final void setUpdateTransaction(){
      this.isReadOnly = false;
   }

   public final void addTakenLock(Object lock){
      if(!this.takenLocks.containsKey(lock))
         this.takenLocks.put(lock, System.nanoTime());
   }

   public final void addValue(IspnStats param, double value){
      try{
         int index = this.getIndex(param);
         this.statisticsContainer.addValue(index,value);
         log.tracef("Add %s to %s", value, param);
      } catch(NoIspnStatException e){
         log.warnf(e, "Exception caught when trying to add the value %s to %s.", value, param);
      }
   }

   public final long getValue(IspnStats param){
      int index = this.getIndex(param);
      long value = this.statisticsContainer.getValue(index);
      log.tracef("Value of %s is %s", param, value);
      return value;
   }

   public final void incrementValue(IspnStats param){
      this.addValue(param,1);
   }

   public final void terminateTransaction() {
      log.tracef("Terminating transaction. Is read only? %s. Is commit? %s", isReadOnly, isCommit);
      long now = System.nanoTime();
      double execTime = now - this.initTime;
      if(this.isReadOnly){
         if(isCommit){
            this.incrementValue(IspnStats.NUM_COMMITTED_RO_TX);
            this.addValue(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME,execTime);
         } else{
            this.incrementValue(IspnStats.NUM_ABORTED_RO_TX);
            this.addValue(IspnStats.RO_TX_ABORTED_EXECUTION_TIME,execTime);
         }
      } else{
         if(isCommit){
            this.incrementValue(IspnStats.NUM_COMMITTED_WR_TX);
            this.addValue(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME,execTime);
         } else{
            this.incrementValue(IspnStats.NUM_ABORTED_WR_TX);
            this.addValue(IspnStats.WR_TX_ABORTED_EXECUTION_TIME,execTime);
         }
      }

      int heldLocks = this.takenLocks.size();
      double cumulativeLockHoldTime = this.computeCumulativeLockHoldTime(heldLocks,now);
      this.addValue(IspnStats.NUM_HELD_LOCKS,heldLocks);
      this.addValue(IspnStats.LOCK_HOLD_TIME,cumulativeLockHoldTime);

      terminate();
   }

   public final void flush(TransactionStatistics ts){
      log.tracef("Flush this [%s] to %s", this, ts);
      this.statisticsContainer.mergeTo(ts.statisticsContainer);
   }

   public final void dump(){
      this.statisticsContainer.dump();
   }

   @Override
   public String toString() {
      return "initTime=" + initTime +
            ", isReadOnly=" + isReadOnly +
            ", isCommit=" + isCommit +
            ", transactionalClass=" + transactionalClass +
            '}';
   }

   protected final int getCommonIndex(IspnStats stat){
      switch (stat){
         case LOCK_HOLD_TIME:
            return LocalRemoteStatistics.LOCK_HOLD_TIME;
         case  NUM_HELD_LOCKS:
            return LocalRemoteStatistics.NUM_HELD_LOCK;
         case LOCK_WAITING_TIME:
            return LocalRemoteStatistics.LOCK_WAITING_TIME;
         case ROLLBACK_EXECUTION_TIME:
            return LocalRemoteStatistics.ROLLBACK_EXECUTION_TIME;
         case COMMIT_EXECUTION_TIME:
            return LocalRemoteStatistics.COMMIT_EXECUTION_TIME;
         case NUM_COMMIT_COMMAND:
            return LocalRemoteStatistics.NUM_COMMIT_COMMAND;
         case NUM_ROLLBACKS:
            return LocalRemoteStatistics.NUM_ROLLBACKS;
         case NUM_PUTS:
            return LocalRemoteStatistics.NUM_PUTS;
         case NUM_SUCCESSFUL_RTTS:
            return LocalRemoteStatistics.NUM_SUCCESSFUL_RTTS;
         case NUM_WAITED_FOR_LOCKS:
            return LocalRemoteStatistics.NUM_WAITED_FOR_LOCKS;
         case NUM_COMMITTED_RO_TX:
            return LocalRemoteStatistics.NUM_COMMITTED_RO_TX;
         case NUM_ABORTED_RO_TX:
            return LocalRemoteStatistics.NUM_ABORTED_RO_TX;
         case NUM_ABORTED_WR_TX:
            return LocalRemoteStatistics.NUM_ABORTED_WR_TX;
         case NUM_COMMITTED_WR_TX:
            return LocalRemoteStatistics.NUM_COMMITTED_WR_TX;
         case LOCK_CONTENTION_TO_LOCAL:
            return LocalRemoteStatistics.LOCK_CONTENTION_TO_LOCAL;
         case LOCK_CONTENTION_TO_REMOTE:
            return LocalRemoteStatistics.LOCK_CONTENTION_TO_REMOTE;
         case NUM_REMOTE_GET:
            return LocalRemoteStatistics.NUM_REMOTE_GET;
         case NUM_REMOTE_PUT:
            return LocalRemoteStatistics.NUM_REMOTE_PUT;
         case REMOTE_GET_EXECUTION:
            return LocalRemoteStatistics.REMOTE_GET_EXECUTION;
         case REMOTE_PUT_EXECUTION:
            return LocalRemoteStatistics.REMOTE_PUT_EXECUTION;
         case WR_TX_ABORTED_EXECUTION_TIME:
            return LocalRemoteStatistics.WR_TX_ABORTED_EXECUTION_TIME;
         case RO_TX_ABORTED_EXECUTION_TIME:
            return LocalRemoteStatistics.RO_TX_ABORTED_EXECUTION_TIME;
         case WR_TX_SUCCESSFUL_EXECUTION_TIME:
            return LocalRemoteStatistics.WR_TX_SUCCESSFUL_EXECUTION_TIME;
         case RO_TX_SUCCESSFUL_EXECUTION_TIME:
            return LocalRemoteStatistics.RO_TX_SUCCESSFUL_EXECUTION_TIME;
      }
      return NON_COMMON_STAT;
   }

   protected abstract int getIndex(IspnStats param);

   protected abstract void onPrepareCommand();

   protected abstract void terminate();

   private long computeCumulativeLockHoldTime(int numLocks,long currentTime){
      long ret = numLocks * currentTime;
      for(Object o:this.takenLocks.keySet())
         ret-=this.takenLocks.get(o);
      return ret;
   }
}

