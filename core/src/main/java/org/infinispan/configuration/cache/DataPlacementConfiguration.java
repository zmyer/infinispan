package org.infinispan.configuration.cache;

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.dataplacement.keyfeature.KeyFeatureManager;
import org.infinispan.dataplacement.lookup.ObjectLookupFactory;
import org.infinispan.util.TypedProperties;

/**
 * Configures the Data Placement optimization
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementConfiguration extends AbstractTypedPropertiesConfiguration {

   private final boolean enabled;
   private final int coolDownTime;
   private final ObjectLookupFactory objectLookupFactory;
   private final KeyFeatureManager keyFeatureManager;

   protected DataPlacementConfiguration(TypedProperties properties, boolean enabled, int coolDownTime,
                                        ObjectLookupFactory objectLookupFactory, KeyFeatureManager keyFeatureManager) {
      super(properties);
      this.enabled = enabled;
      this.coolDownTime = coolDownTime;
      this.objectLookupFactory = objectLookupFactory;
      this.keyFeatureManager = keyFeatureManager;
   }

   public ObjectLookupFactory objectLookupFactory() {
      return objectLookupFactory;
   }

   public KeyFeatureManager keyFeatureManager() {
      return keyFeatureManager;
   }

   public boolean enabled() {
      return enabled;
   }

   public int coolDownTime() {
      return coolDownTime;
   }

   @Override
   public String toString() {
      return "DataPlacementConfiguration{" +
            "enabled=" + enabled +
            ", coolDownTime=" + coolDownTime +
            ", objectLookupFactory=" + objectLookupFactory +
            ", keyFeatureManager=" + keyFeatureManager +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DataPlacementConfiguration)) return false;
      if (!super.equals(o)) return false;

      DataPlacementConfiguration that = (DataPlacementConfiguration) o;

      if (coolDownTime != that.coolDownTime) return false;
      if (enabled != that.enabled) return false;
      if (keyFeatureManager != null ? !keyFeatureManager.equals(that.keyFeatureManager) : that.keyFeatureManager != null)
         return false;
      if (objectLookupFactory != null ? !objectLookupFactory.equals(that.objectLookupFactory) : that.objectLookupFactory != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (enabled ? 1 : 0);
      result = 31 * result + coolDownTime;
      result = 31 * result + (objectLookupFactory != null ? objectLookupFactory.hashCode() : 0);
      result = 31 * result + (keyFeatureManager != null ? keyFeatureManager.hashCode() : 0);
      return result;
   }
}
