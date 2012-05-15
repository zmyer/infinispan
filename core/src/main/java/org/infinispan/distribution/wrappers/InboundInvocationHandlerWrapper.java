package org.infinispan.distribution.wrappers;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Pedro Ruivo
 * @since 5.2
 */
public class InboundInvocationHandlerWrapper implements InboundInvocationHandler {

   private final InboundInvocationHandler actual;
   private static final Log log = LogFactory.getLog(InboundInvocationHandlerWrapper.class);
   
   private final TransactionTable transactionTable;

   public InboundInvocationHandlerWrapper(InboundInvocationHandler actual, TransactionTable transactionTable) {
      this.actual = actual;
      this.transactionTable = transactionTable;
   }     

   @Override
   public Response handle(CacheRpcCommand command, Address origin) throws Throwable {
      log.tracef("Handle remote command [%s] by the invocation handle wrapper from %s", command, origin);
      long currTime = System.nanoTime();
      GlobalTransaction globalTransaction = getGlobalTransaction(command);
      try{
         if (globalTransaction != null) {
            log.debugf("The command %s is transactional and the global transaction is %s", command, globalTransaction);
            TransactionsStatisticsRegistry.attachRemoteTransactionStatistic(globalTransaction);
         } else {
            log.debugf("The command %s is NOT transactional", command);
         }

         if(command instanceof PrepareCommand){
            Response ret = actual.handle(command,origin);
            TransactionsStatisticsRegistry.addValue(IspnStats.REPLAY_TIME,System.nanoTime() - currTime);
            TransactionsStatisticsRegistry.incrementValue(IspnStats.REPLAYED_TXS);
            return ret;
         } else {
            return actual.handle(command,origin);
         }
      } finally {         
         if (globalTransaction != null) {
            log.debugf("Detach statistics for command %s", command, globalTransaction);
            TransactionsStatisticsRegistry.detachRemoteTransactionStatistic(globalTransaction,
                                                                            !transactionTable.containRemoteTx(globalTransaction));
         }
      }
   }

   private GlobalTransaction getGlobalTransaction(CacheRpcCommand cacheRpcCommand) {
      if (cacheRpcCommand instanceof TransactionBoundaryCommand) {
         return ((TransactionBoundaryCommand) cacheRpcCommand).getGlobalTransaction();
      } else if (cacheRpcCommand instanceof TxCompletionNotificationCommand) {
         for (Object obj : cacheRpcCommand.getParameters()) {
            if (obj instanceof GlobalTransaction) {
               return (GlobalTransaction) obj;
            }
         }
      }
      return null;
   }
}
