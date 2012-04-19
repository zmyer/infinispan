package org.infinispan.distribution.wrappers;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
public class InboundInvocationHandlerWrapper implements InboundInvocationHandler {

   private final InboundInvocationHandler actual;

   public InboundInvocationHandlerWrapper(InboundInvocationHandler actual) {
      this.actual = actual;
   }

   @Override
   public Response handle(CacheRpcCommand command, Address origin) throws Throwable {
      System.out.println("InboundInvocationHandlerWrapper.handle");
      return actual.handle(command, origin);
   }
}
