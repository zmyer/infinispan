package org.infinispan.stats.translations;

/**
 * Websiste: www.cloudtm.eu
 * Date: 01/05/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class RemoteStatistics extends LocalRemoteStatistics {
   private static final int NUM_ONLY_REMOTE_STATS = 2;
   private static final int offset = LocalRemoteStatistics.numLocalRemoteStatistcs;
   public static final int NUM_STATS = offset + NUM_ONLY_REMOTE_STATS;


   public static final int REPLAY_TIME = offset;
   public static final int REPLAYED_TXS = offset + 1;
}
