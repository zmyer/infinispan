package org.infinispan.stats;

import org.infinispan.stats.translations.ExposedStatistics;
import org.infinispan.stats.translations.RemoteStatistics;

/**
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class RemoteTransactionStatistics extends TransactionStatistics{

   public RemoteTransactionStatistics(){
      super();
      this.statisticsContainer = new StatisticsContainerImpl(RemoteStatistics.NUM_STATS);
   }

   protected final void onPrepareCommand(){
      //nop
   }

   protected final int getIndex(ExposedStatistics.IspnStats stat) throws NoIspnStatException{
      int ret = super.getCommonIndex(stat);
      if(ret!=NON_COMMON_STAT)
         return ret;
      switch (stat){
         case REPLAY_TIME:
            return RemoteStatistics.REPLAY_TIME;
         case REPLAYED_TXS:
            return RemoteStatistics.REPLAYED_TXS;
         default:
            throw new NoIspnStatException("Statistic "+stat+" is not available!");
      }
   }

   @Override
   public String toString() {
      return "RemoteTransactionStatistics{" + super.toString();
   }
}
