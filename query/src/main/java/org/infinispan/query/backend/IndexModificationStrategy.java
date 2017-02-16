package org.infinispan.query.backend;

import org.hibernate.search.spi.IndexingMode;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.context.impl.FlagBitSets;

/**
 * Defines for which events the Query Interceptor will generate indexing events.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
public enum IndexModificationStrategy {

   /**
    * Indexing events will not be triggered automatically, still the indexing service
    * will be available.
    * Suited for example if index updates are controlled explicitly.
    */
   MANUAL {
      @Override
      public boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx,
                                         DistributionManager distributionManager, RpcManager rpcManager, Object key) {
         return false;
      }
   },

   /**
    * Any event intercepted by the current node will trigger an indexing event
    * (excepting those flagged with {@code Flag.SKIP_INDEXING}.
    */
   ALL {
      @Override
      public boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx,
                                         DistributionManager distributionManager, RpcManager rpcManager, Object key) {
         return !command.hasAnyFlag(FlagBitSets.SKIP_INDEXING);
      }
   },

   /**
    * Only events generated by the current node will trigger indexing events
    * (excepting those flagged with {@code Flag.SKIP_INDEXING}.
    */
   LOCAL_ONLY {
      @Override
      public boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx,
                                         DistributionManager distributionManager, RpcManager rpcManager, Object key) {
         // will index only local updates that were not flagged with SKIP_INDEXING,
         // are not caused internally by state transfer and indexing strategy is not configured to 'manual'
         return ctx.isOriginLocal() && !command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER | FlagBitSets.SKIP_INDEXING);
      }
   },

   /**
    * Only events target to the primary owner will trigger indexing of the data
    */
   PRIMARY_OWNER {
      @Override
      public boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx,
                                         DistributionManager distributionManager, RpcManager rpcManager, Object key) {
         return command instanceof ClearCommand ||
               !(command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER) || command.hasAnyFlag(FlagBitSets.SKIP_INDEXING)) &&
                     (distributionManager == null || distributionManager.getPrimaryLocation(key).equals(rpcManager.getAddress()));

      }
   };

   public abstract boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx,
                                               DistributionManager distributionManager, RpcManager rpcManager, Object key);

   /**
    * For a given configuration, define which IndexModificationStrategy is going to be used.
    * @param searchFactory
    * @param cfg
    * @return the appropriate IndexModificationStrategy
    */
   public static IndexModificationStrategy configuredStrategy(SearchIntegrator searchFactory, Configuration cfg) {
      IndexingMode indexingMode = searchFactory.unwrap(SearchIntegrator.class).getIndexingMode();
      if (indexingMode == IndexingMode.MANUAL) {
         return MANUAL;
      } else {
         if (cfg.indexing().index().isLocalOnly()) {
            return LOCAL_ONLY;
         } else if (cfg.indexing().index().isPrimaryOwner()) {
            return PRIMARY_OWNER;
         } else {
            return ALL;
         }
      }
   }

}
