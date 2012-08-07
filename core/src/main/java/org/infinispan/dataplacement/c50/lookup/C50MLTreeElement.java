package org.infinispan.dataplacement.c50.lookup;

import org.infinispan.dataplacement.keyfeature.AbstractFeature;
import org.infinispan.dataplacement.keyfeature.FeatureValue;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Represents an element (i.e. it is a rule) in the tree
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class C50MLTreeElement implements Serializable {

   private int ownerIndex;
   private AbstractFeature feature;
   private FeatureValue featureValue;
   private FeatureValue.Condition condition;
   private final List<C50MLTreeElement> children;

   public C50MLTreeElement(int ownerIndex, AbstractFeature feature, String value, String condition) {
      this.ownerIndex = ownerIndex;
      this.feature = feature;
      this.featureValue = feature.createFeatureValueFromString(value);
      this.condition = FeatureValue.Condition.fromString(condition);
      this.children = new LinkedList<C50MLTreeElement>();
   }

   public C50MLTreeElement() {
      this.children = new LinkedList<C50MLTreeElement>();
   } //to externalize/serialize

   /**
    * returns the owner index. it is only valid if the condition is true
    *
    * @return  the owner index
    */
   public int getOwnerIndex() {
      return ownerIndex;
   }

   /**
    * returns the children elements (rules). it is valid when the condition is true
    *
    * @return  the children elements
    */
   public List<C50MLTreeElement> getChildren() {
      return children;
   }

   /**
    * adds a child to this element
    *
    * @param child   the tree element
    */
   public void addChild(C50MLTreeElement child) {
      children.add(child);
   }

   /**
    * checks if the key features matches with the condition in this element
    *
    * @param keyFeatures   the key features
    * @return              true if the key features matches, false otherwise
    */
   public boolean hasMatch(Map<AbstractFeature, FeatureValue> keyFeatures) {
      return feature != null && featureValue.valueMatch(keyFeatures.get(feature), condition);
   }

   /**
    * returns true if this is a leaf element in the tree (i.e. contains the new owner index)
    *
    * @return  true if this is a leaf element, false otherwise
    */
   public boolean isLeaf() {
      return ownerIndex != -1;
   }

   @Override
   public String toString() {
      return "C50MLTreeElement{" +
            "ownerIndex=" + ownerIndex +
            ", feature=" + feature +
            ", featureValue=" + featureValue +
            ", condition=" + condition +
            ", children=" + children +
            '}';
   }
}
