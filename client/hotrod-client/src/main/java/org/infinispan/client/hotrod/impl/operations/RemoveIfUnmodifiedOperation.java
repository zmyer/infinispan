package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "removeIfUnmodified" operation as defined by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class RemoveIfUnmodifiedOperation<V> extends AbstractKeyOperation<VersionedOperationResponse<V>> {

   private final long version;

   public RemoveIfUnmodifiedOperation(Codec codec, ChannelFactory channelFactory,
                                      Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId,
                                      int flags, Configuration cfg,
                                      long version) {
      super(codec, channelFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
      this.version = version;
   }

   @Override
   protected void executeOperation(Channel channel) {
      HeaderParams header = headerParams(REMOVE_IF_UNMODIFIED_REQUEST);
      scheduleRead(channel, header);

      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(keyBytes) + 8);

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, keyBytes);
      buf.writeLong(version);
      channel.writeAndFlush(buf);
   }

   @Override
   public VersionedOperationResponse<V> decodePayload(ByteBuf buf, short status) {
      return returnVersionedOperationResponse(buf, status);
   }
}
