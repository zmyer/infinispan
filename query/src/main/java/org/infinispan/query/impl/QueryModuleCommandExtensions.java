package org.infinispan.query.impl;

import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices
public final class QueryModuleCommandExtensions implements ModuleCommandExtensions {

   @Override
   public ModuleCommandFactory getModuleCommandFactory() {
      return new CommandFactory();
   }
}
