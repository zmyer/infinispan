package org.infinispan.stats;

import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.stats.translations.LocalStatistics;

/**
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class LocalTransactionStatistics extends TransactionStatistics {

   private boolean stillLocalExecution;

   public LocalTransactionStatistics(){
      super();
      this.stillLocalExecution = true;
      this.statisticsContainer = new StatisticsContainerImpl(LocalStatistics.NUM_STATS);
   }

   public final void terminateLocalExecution(){
      this.stillLocalExecution = false;
      this.addValue(IspnStats.WR_TX_LOCAL_EXECUTION_TIME,System.nanoTime() - this.initTime);
      this.incrementValue(IspnStats.NUM_PREPARES);
   }

   public final boolean isStillLocalExecution(){
      return this.stillLocalExecution;
   }

   protected final void onPrepareCommand(){
      this.terminateLocalExecution();
   }

   protected final int getIndex(IspnStats stat) throws NoIspnStatException{
      int ret = super.getCommonIndex(stat);
      if(ret!=NON_COMMON_STAT)
         return ret;

      switch (stat){
         case LOCAL_CONTENTION_PROBABILITY:
            return LocalStatistics.LOCAL_CONTENTION_PROBABILITY;
         case WR_TX_LOCAL_EXECUTION_TIME:
            return LocalStatistics.WR_TX_LOCAL_EXECUTION_TIME;
         case WR_TX_SUCCESSFUL_EXECUTION_TIME:
            return LocalStatistics.WR_TX_SUCCESSFUL_LOCAL_EXECUTION_TIME;
         case NUM_SUCCESSFUL_PUTS:
            return LocalStatistics.PUTS_PER_LOCAL_TX;
         case PREPARE_COMMAND_SIZE:
            return LocalStatistics.PREPARE_COMMAND_SIZE;
         case NUM_NODES_IN_PREPARE:
            return LocalStatistics.NUM_NODES_IN_PREPARE;
         case NUM_PREPARES:
            return LocalStatistics.NUM_PREPARE;
         case RTT:
            return LocalStatistics.RTT;
         case NUM_SUCCESSFUL_RTTS:
            return LocalStatistics.NUM_RTTS;
         default:
            throw new NoIspnStatException("IspnStats "+stat+" not found!");
      }
   }

   @Override
   public String toString() {
      return "LocalTransactionStatistics{" +
            "stillLocalExecution=" + stillLocalExecution +
            ", " + super.toString();
   }
}
