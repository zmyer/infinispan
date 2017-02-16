package org.infinispan.jcache;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.jcache.logging.Log;

/**
 * Failure entry processor result wrapper.
 *
 * @author Galder Zamarreño
 */
public class FailureEntryProcessorResult<T> implements EntryProcessorResult<T> {

   private static final Log log = LogFactory.getLog(FailureEntryProcessorResult.class, Log.class);

   private final Throwable t;

   public FailureEntryProcessorResult(Throwable t) {
      this.t = t;
   }

   @Override
   public T get() throws EntryProcessorException {
      throw log.entryProcessingFailed(t);
   }

}
