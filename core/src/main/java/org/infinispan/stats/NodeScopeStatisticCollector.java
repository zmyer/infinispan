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
         case RTT:{
            long numRtts = localTransactionStatistics.getValue(IspnStats.NUM_SUCCESSFUL_RTTS);
            if(numRtts!=0){
               long rtt = localTransactionStatistics.getValue(IspnStats.RTT);
               return new Long(rtt / numRtts);
            }
            return new Long(0);
         }
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
         case PREPARE_COMMAND_SIZE:{
            long numPrepares = localTransactionStatistics.getValue(IspnStats.NUM_PREPARES);
            if(numPrepares!=0){
               long prepareCommandSize = localTransactionStatistics.getValue(IspnStats.PREPARE_COMMAND_SIZE);
               return new Long(prepareCommandSize / numPrepares);
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
         default:
            throw new NoIspnStatException("Invalid statistic "+param);
      }
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
