package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import net.jcip.annotations.Immutable;

/**
 * Corresponds to clear operation as defined by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class ClearOperation extends RetryOnFailureOperation<Void> {

   public ClearOperation(Codec codec, TransportFactory transportFactory,
                         byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, transportFactory, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected Void executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, CLEAR_REQUEST);
      transport.flush();

      readHeaderAndValidate(transport, params);
      return null;
   }
}
