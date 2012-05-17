package org.infinispan.stats;

import org.infinispan.stats.percentiles.PercentileStats;
import org.infinispan.stats.percentiles.PercentileStatsFactory;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * Websiste: www.cloudtm.eu
 * Date: 01/05/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NodeScopeStatisticCollector {
   private final static Log log = LogFactory.getLog(NodeScopeStatisticCollector.class);

   private LocalTransactionStatistics localTransactionStatistics;
   private RemoteTransactionStatistics remoteTransactionStatistics;

   private PercentileStats localTransactionWrExecutionTime;
   private PercentileStats remoteTransactionWrExecutionTime;
   private PercentileStats localTransactionRoExecutionTime;
   private PercentileStats remoteTransactionRoExecutionTime;

   private long lastResetTime;

   public final synchronized void reset(){
      log.tracef("Resetting Node Scope Statistics");
      this.localTransactionStatistics = new LocalTransactionStatistics();
      this.remoteTransactionStatistics = new RemoteTransactionStatistics();

      this.localTransactionRoExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.localTransactionWrExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.remoteTransactionRoExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.remoteTransactionWrExecutionTime = PercentileStatsFactory.createNewPercentileStats();

      this.lastResetTime = System.nanoTime();
   }

   public NodeScopeStatisticCollector(){
      reset();
   }

   public final synchronized void merge(TransactionStatistics ts){
      log.tracef("Merge transaction statistics %s to the node statistics", ts);
      if(ts instanceof LocalTransactionStatistics){
         ts.flush(this.localTransactionStatistics);
         if(ts.isCommit()){
            if(ts.isReadOnly()){
               this.localTransactionRoExecutionTime.insertSample(ts.getValue(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME));
            }
            else{
               this.localTransactionWrExecutionTime.insertSample(ts.getValue(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME));
            }
         }
      }
      else if(ts instanceof RemoteTransactionStatistics){
         ts.flush(this.remoteTransactionStatistics);
         if(ts.isCommit()){
            if(ts.isReadOnly()){
               this.remoteTransactionRoExecutionTime.insertSample(ts.getValue(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME));
            }
            else{
               this.remoteTransactionWrExecutionTime.insertSample(ts.getValue(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME));
            }
         }
      }
   }

   //TODO double check sul synchronized e inserire il controllo anti-divisione per zero
   @SuppressWarnings("UnnecessaryBoxing")
   public synchronized Object getAttribute(IspnStats param) throws NoIspnStatException{
      log.tracef("Get attribute %s", param);
      switch (param) {
         case LOCAL_EXEC_NO_CONT:{
            //TODO you have to compute this when you flush the transaction (for Diego)
            long numLocalTxToPrepare = localTransactionStatistics.getValue(IspnStats.NUM_PREPARES);
            if(numLocalTxToPrepare!=0){
               long localExec = localTransactionStatistics.getValue(IspnStats.WR_TX_LOCAL_EXECUTION_TIME);
               long waitTime = localTransactionStatistics.getValue(IspnStats.LOCK_WAITING_TIME);
               return new Long((localExec - waitTime) / numLocalTxToPrepare);
            }
            return new Long(0);
         }
         case LOCK_HOLD_TIME:{
            long localLocks = localTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            long remoteLocks = remoteTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            if((localLocks + remoteLocks) !=0){
               long localHoldTime = localTransactionStatistics.getValue(IspnStats.LOCK_HOLD_TIME);
               long remoteHoldTime = remoteTransactionStatistics.getValue(IspnStats.LOCK_HOLD_TIME);
               return new Long((localHoldTime + remoteHoldTime) / (localLocks + remoteLocks));
            }
            return new Long(0);
         }
         case RTT_PREPARE:
            return avg(IspnStats.NUM_RTTS_PREPARE, IspnStats.RTT_PREPARE);
         case RTT_COMMIT:
            return avg(IspnStats.NUM_RTTS_COMMIT, IspnStats.RTT_COMMIT);
         case RTT_ROLLBACK:
            return avg(IspnStats.NUM_RTTS_ROLLBACK, IspnStats.RTT_ROLLBACK);
         case RTT_GET:
            return avg(IspnStats.NUM_RTTS_PREPARE, IspnStats.RTT_GET);
         case ASYNC_COMMIT:
            return avg(IspnStats.NUM_ASYNC_COMMIT, IspnStats.ASYNC_COMMIT);
         case ASYNC_COMPLETE_NOTIFY:
            return avg(IspnStats.NUM_ASYNC_COMPLETE_NOTIFY, IspnStats.ASYNC_COMPLETE_NOTIFY);
         case ASYNC_PREPARE:
            return avg(IspnStats.NUM_ASYNC_PREPARE, IspnStats.ASYNC_PREPARE);
         case ASYNC_ROLLBACK:
            return avg(IspnStats.NUM_ASYNC_ROLLBACK, IspnStats.ASYNC_ROLLBACK);
         case NUM_NODES_COMMIT:
            return avgMultipleCounters(IspnStats.NUM_NODES_COMMIT, IspnStats.NUM_RTTS_COMMIT, IspnStats.NUM_ASYNC_COMMIT);
         case NUM_NODES_GET:
            return avgMultipleCounters(IspnStats.NUM_NODES_GET, IspnStats.NUM_RTTS_GET);
         case NUM_NODES_PREPARE:
            return avgMultipleCounters(IspnStats.NUM_NODES_PREPARE, IspnStats.NUM_RTTS_PREPARE, IspnStats.NUM_ASYNC_PREPARE);
         case NUM_NODES_ROLLBACK:
            return avgMultipleCounters(IspnStats.NUM_NODES_ROLLBACK, IspnStats.NUM_RTTS_ROLLBACK, IspnStats.NUM_ASYNC_ROLLBACK);
         case NUM_NODES_COMPLETE_NOTIFY:
            return avgMultipleCounters(IspnStats.NUM_NODES_COMPLETE_NOTIFY, IspnStats.NUM_ASYNC_COMPLETE_NOTIFY);
         case PUTS_PER_LOCAL_TX:{
            long numLocalTxToPrepare = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            if(numLocalTxToPrepare!=0){
               long numSuccessfulPuts = localTransactionStatistics.getValue(IspnStats.NUM_SUCCESSFUL_PUTS);
               return new Long(numSuccessfulPuts / numLocalTxToPrepare);
            }
            return new Long(0);

         }
         case LOCAL_CONTENTION_PROBABILITY:{
            long numLocalPuts = localTransactionStatistics.getValue(IspnStats.NUM_PUTS);
            if(numLocalPuts != 0){
               long numLocalLocalContention = localTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
               long numLocalRemoteContention = localTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_REMOTE);
               return new Double((numLocalLocalContention + numLocalRemoteContention) * 1.0 / numLocalPuts);
            }
            return new Double(0);
         }
         case COMMIT_EXECUTION_TIME:{
            long numCommits = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX) +
                  localTransactionStatistics.getIndex(IspnStats.NUM_COMMITTED_RO_TX);
            if(numCommits!=0){
               long commitExecTime = localTransactionStatistics.getValue(IspnStats.COMMIT_EXECUTION_TIME);
               return new Long(commitExecTime / numCommits);
            }
            return new Long(0);

         }
         case ROLLBACK_EXECUTION_TIME:{
            long numRollbacks = localTransactionStatistics.getValue(IspnStats.NUM_ROLLBACKS);
            if(numRollbacks != 0){
               long rollbackExecTime = localTransactionStatistics.getValue(IspnStats.ROLLBACK_EXECUTION_TIME);
               return new Long(rollbackExecTime / numRollbacks);
            }
            return new Long(0);

         }
         case LOCK_WAITING_TIME:{
            long localWaitedForLocks = localTransactionStatistics.getValue(IspnStats.NUM_WAITED_FOR_LOCKS);
            long remoteWaitedForLocks = remoteTransactionStatistics.getValue(IspnStats.NUM_WAITED_FOR_LOCKS);
            long totalWaitedForLocks = localWaitedForLocks + remoteWaitedForLocks;
            if(totalWaitedForLocks!=0){
               long localWaitedTime = localTransactionStatistics.getValue(IspnStats.LOCK_WAITING_TIME);
               long remoteWaitedTime = remoteTransactionStatistics.getIndex(IspnStats.LOCK_WAITING_TIME);
               return new Long((localWaitedTime + remoteWaitedTime) / totalWaitedForLocks);
            }
            return new Long(0);
         }
         case REPLAY_TIME:{
            long numReplayed = remoteTransactionStatistics.getValue(IspnStats.NUM_REPLAYED_TXS);
            if(numReplayed!=0){
               long replayTime = remoteTransactionStatistics.getValue(IspnStats.REPLAY_TIME);
               return new Long(replayTime / numReplayed);
            }
            return new Long(0);
         }
         case ARRIVAL_RATE:{
            long localCommittedTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
                  localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            long localAbortedTx = localTransactionStatistics.getValue(IspnStats.NUM_ABORTED_RO_TX) +
                  localTransactionStatistics.getValue(IspnStats.NUM_ABORTED_WR_TX);
            long remoteCommittedTx = remoteTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
                  remoteTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            long remoteAbortedTx = remoteTransactionStatistics.getValue(IspnStats.NUM_ABORTED_RO_TX) +
                  remoteTransactionStatistics.getValue(IspnStats.NUM_ABORTED_WR_TX);
            long totalBornTx = localAbortedTx + localCommittedTx + remoteAbortedTx + remoteCommittedTx;
            return new Long((long) (totalBornTx / convertNanosToSeconds(System.nanoTime() - this.lastResetTime)));
         }
         case TX_WRITE_PERCENTAGE:{     //computed on the locally born txs
            long readTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
                  localTransactionStatistics.getValue(IspnStats.NUM_ABORTED_RO_TX);
            long writeTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX) +
                  localTransactionStatistics.getValue(IspnStats.NUM_ABORTED_WR_TX);
            long total = readTx + writeTx;
            if(total!=0)
               return new Double(writeTx * 1.0 / total);
            return new Double(0);
         }
         case SUCCESSFUL_WRITE_PERCENTAGE:{ //computed on the locally born txs
            long readSuxTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX);
            long writeSuxTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            long total = readSuxTx + writeSuxTx;
            if(total!=0){
               return new Double(writeSuxTx * 1.0 / total);
            }
            return new Double(0);
         }
         case APPLICATION_CONTENTION_FACTOR:{
            long localTakenLocks = localTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            long remoteTakenLocks = remoteTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            long elapsedTime = System.nanoTime() - this.lastResetTime;
            double totalLocksArrivalRate = (localTakenLocks + remoteTakenLocks) / convertNanosToMicro(elapsedTime);
            if(totalLocksArrivalRate!=0){
               double localContProb = (Double) this.getAttribute(IspnStats.LOCAL_CONTENTION_PROBABILITY);
               long holdTime = (Long)this.getAttribute(IspnStats.LOCK_HOLD_TIME);
               return new Double(localContProb * holdTime / totalLocksArrivalRate);
            }
            return new Double(0);
         }
         case NUM_LOCK_FAILED_DEADLOCK:
         case NUM_LOCK_FAILED_TIMEOUT:
            return new Long(localTransactionStatistics.getValue(param));
         case WR_TX_LOCAL_EXECUTION_TIME:
            return avg(IspnStats.NUM_PREPARES, IspnStats.WR_TX_LOCAL_EXECUTION_TIME);
         case WR_TX_SUCCESSFUL_EXECUTION_TIME:
            return avg(IspnStats.NUM_COMMITTED_WR_TX, IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME);
         case RO_TX_SUCCESSFUL_EXECUTION_TIME:
            return avg(IspnStats.NUM_COMMITTED_RO_TX, IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME);
         case PREPARE_COMMAND_SIZE:
            return avgMultipleCounters(IspnStats.PREPARE_COMMAND_SIZE, IspnStats.NUM_RTTS_PREPARE, IspnStats.NUM_ASYNC_PREPARE);
         case COMMIT_COMMAND_SIZE:
            return avgMultipleCounters(IspnStats.COMMIT_COMMAND_SIZE, IspnStats.NUM_RTTS_COMMIT, IspnStats.NUM_ASYNC_COMMIT);
         case CLUSTERED_GET_COMMAND_SIZE:
            return avg(IspnStats.NUM_RTTS_GET, IspnStats.CLUSTERED_GET_COMMAND_SIZE);
         default:
            throw new NoIspnStatException("Invalid statistic "+param);
      }
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Long avg(IspnStats counter, IspnStats duration) {
      long num = localTransactionStatistics.getValue(counter);
      if (num != 0) {
         long dur = localTransactionStatistics.getValue(duration);
         return new Long(dur / num);
      }
      return new Long(0);
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Long avgMultipleCounters(IspnStats duration, IspnStats... counters) {
      long num = 0;
      for (IspnStats counter : counters) {
         num += localTransactionStatistics.getValue(counter);
      }
      if (num != 0) {
         long dur = localTransactionStatistics.getValue(duration);
         return new Long(dur / num);
      }
      return new Long(0);
   }

   private static double convertNanosToMicro(long nanos) {
      return nanos / 1000.0;
   }

   private static double convertNanosToMillis(long nanos) {
      return nanos / 1000000.0;
   }

   private static double convertNanosToSeconds(long nanos) {
      return nanos / 1000000000.0;
   }
}
