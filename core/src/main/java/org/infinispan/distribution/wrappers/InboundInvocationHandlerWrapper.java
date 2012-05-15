package org.infinispan.distribution.wrappers;

import org.apache.log4j.Logger;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.swing.plaf.metal.MetalBorders;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Pedro Ruivo
 * @since 5.2
 */
public class InboundInvocationHandlerWrapper implements InboundInvocationHandler {

   private final InboundInvocationHandler actual;
   Logger log = Logger.getLogger(InboundInvocationHandlerWrapper.class);


   public InboundInvocationHandlerWrapper(InboundInvocationHandler actual) {
      this.actual = actual;
   }

   @Override
   public Response handle(CacheRpcCommand command, Address origin) throws Throwable {
      System.out.println("InboundInvocationHandlerWrapper.handle "+command);
      long currTime = System.nanoTime();
      Response ret;
      boolean shouldAttachTransaction = isTransactionalCommand(command);
      GlobalTransaction globalTransaction = getGlobalTransaction(command);
      if (shouldAttachTransaction) {
         TransactionsStatisticsRegistry.initRemoteTransaction();
      }
      try{
         if(command instanceof PrepareCommand){
            ret = actual.handle(command,origin);
            TransactionsStatisticsRegistry.addValue(IspnStats.REPLAY_TIME,System.nanoTime() - currTime);
            TransactionsStatisticsRegistry.incrementValue(IspnStats.REPLAYED_TXS);
            return ret;
         } else {
            return actual.handle(command,origin);
         }
      } finally {
         if (shouldAttachTransaction) {
            //detach transaction
         }
      }
   }

   private boolean isTransactionalCommand(CacheRpcCommand cacheRpcCommand) {
      return cacheRpcCommand instanceof PrepareCommand || cacheRpcCommand instanceof CommitCommand ||
            cacheRpcCommand instanceof RollbackCommand || cacheRpcCommand instanceof TxCompletionNotificationCommand;
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
