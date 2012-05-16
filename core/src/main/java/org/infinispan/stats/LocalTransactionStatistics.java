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
      super(LocalStatistics.getSize());
      this.stillLocalExecution = true;
   }

   public final void terminateLocalExecution(){
      this.stillLocalExecution = false;
      this.addValue(IspnStats.WR_TX_LOCAL_EXECUTION_TIME,System.nanoTime() - this.initTime);
      this.incrementValue(IspnStats.NUM_PREPARES);
   }

   public final boolean isStillLocalExecution(){
      return this.stillLocalExecution;
   }

   @Override
   protected final void terminate() {
      if (!isReadOnly() && isCommit()) {
         long numPuts = this.getValue(IspnStats.NUM_PUTS);
         this.addValue(IspnStats.NUM_SUCCESSFUL_PUTS,numPuts);
      }
   }

   protected final void onPrepareCommand(){
      this.terminateLocalExecution();
   }

   protected final int getIndex(IspnStats stat) throws NoIspnStatException{
      int ret = LocalStatistics.getIndex(stat);
      if (ret != LocalStatistics.NOT_FOUND) {
         throw new NoIspnStatException("IspnStats "+stat+" not found!");
      }
      return ret;
   }

   @Override
   public final String toString() {
      return "LocalTransactionStatistics{" +
            "stillLocalExecution=" + stillLocalExecution +
            ", " + super.toString();
   }
}
