package org.infinispan.query.remote.impl;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Norms;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.Cache;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapperFieldBridge;
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;
import org.kohsuke.MetaInfServices;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@MetaInfServices
@SuppressWarnings("unused")
public final class ProgrammaticSearchMappingProviderImpl implements ProgrammaticSearchMappingProvider {

   public static final String INDEX_NAME_SUFFIX = "_protobuf";

   @Override
   public void defineMappings(Cache cache, SearchMapping searchMapping) {
      searchMapping.entity(ProtobufValueWrapper.class)
            .indexed()
            .indexName(cache.getName() + INDEX_NAME_SUFFIX)
            .analyzerDiscriminator(ProtobufValueWrapperAnalyzerDiscriminator.class)
            .classBridgeInstance(new ProtobufValueWrapperFieldBridge(cache))
            .norms(Norms.NO)
            .analyze(Analyze.NO)
            .store(Store.NO);
   }
}
