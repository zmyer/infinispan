package org.infinispan.dataplacement;

import org.infinispan.container.DataContainer;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * It is responsible to collect all the remote top accesses (to his own keys) from every member and decide 
 * to where the keys should be moved
 *
 * @author Zhongmiao Li
 * @author João Paiva
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectPlacementManager {

   private static final Log log = LogFactory.getLog(ObjectPlacementManager.class);

   private final StreamLibContainer analyticsBean;

   //contains the list of members (same order in all nodes)
   private final List<Address> addressList;

   //The object that were sent out. The left of pair is the destination, the right is the number that it is moved
   private final Map<Object, Pair<Integer, Integer>> allSentObjects;

   //<node index, <key, number of accesses>
   private final Map<Integer, Map<Object, Long>> requestReceivedMap;

   //<key, destination node index>
   private final Map<Object, Integer> objectsToMove;

   private boolean hasReceivedAllRequests;

   private final DataContainer dataContainer;
   private final DistributionManager distributionManager;

   public ObjectPlacementManager(DistributionManager distributionManager, DataContainer dataContainer){
      analyticsBean = StreamLibContainer.getInstance();
      this.dataContainer = dataContainer;
      this.distributionManager = distributionManager;

      addressList = new ArrayList<Address>();
      allSentObjects = new HashMap<Object, Pair<Integer, Integer>>();
      requestReceivedMap = new TreeMap<Integer, Map<Object, Long>>();
      objectsToMove = new HashMap<Object, Integer>();
      hasReceivedAllRequests = false;
   }

   /**
    * reset the state (before each round)
    */
   public final synchronized void resetState() {
      requestReceivedMap.clear();
      hasReceivedAllRequests = false;
   }

   /**
    * updates the members list
    *
    * @param addresses  the new member list
    */
   public final synchronized void updateMembersList(List<Address> addresses) {
      addressList.clear();
      addressList.addAll(addresses);
   }

   /**
    * collects a remote top access from a member.
    *
    * Note: it returns true only once, on the first time it has all the remote top accessed needed
    *
    * @param sender        the sender
    * @param objectRequest the remote top accesses    
    * @return              true if it has all the remote top accesses needed (see Note)
    */
   public final synchronized boolean aggregateResult(Address sender,Map<Object, Long> objectRequest){
      if (hasReceivedAllRequests) {
         return false;
      }

      int senderID = addressList.indexOf(sender);

      if (senderID < 0) {
         log.warnf("Received request list from %s but it does not exits", sender);
         return false;
      }

      requestReceivedMap.put(senderID, objectRequest);
      hasReceivedAllRequests = addressList.size() <= requestReceivedMap.size();

      if (log.isDebugEnabled()) {
         log.debugf("Received request list from %s. Received from %s nodes and expects %s. Keys are %s", sender,
                    requestReceivedMap.size(), addressList.size(), objectRequest);
      } else {
         log.infof("Received request list from %s. Received from %s nodes and expects %s", sender,
                   requestReceivedMap.size(), addressList.size());
      }

      return hasReceivedAllRequests;
   }

   /**
    * calculates (only once) where the objects should be moved
    *
    * @return  a map with each object and the new owner index 
    */
   public final synchronized Map<Object, Integer> getObjectsToMove() {
      Map<Object, Pair<Long, Integer>> fullRequestList = compactRequestList();
      populateObjectToMove(fullRequestList);
      return objectsToMove;
   }

   /**
    * gives information about the error in Object Lookup created
    *
    * @param objectLookup  the Object Lookup instance to test
    */
   public final void testObjectLookup(ObjectLookup objectLookup) {
      log.warn("Testing bloom filter before sending!");
      int bfErrorCount = 0;
      for(Entry<Object, Integer> entry : objectsToMove.entrySet()) {
         if(objectLookup.query(entry.getKey()) != entry.getValue()){
            ++bfErrorCount;
         }
      }
      log.warn("Error of look upper before sending :"+bfErrorCount);
   }

   /**
    * Merge the request lists from all other nodes into a single request list
    *
    * @return  a map with the object and the corresponding node index and number of accesses (the higher number of
    *          accesses)
    */
   private Map<Object, Pair<Long, Integer>> compactRequestList() {
      Map<Object, Pair<Long, Integer>> fullRequestList = new HashMap<Object, Pair<Long, Integer>>();

      Iterator<Entry<Integer, Map<Object, Long>>> iterator = requestReceivedMap.entrySet().iterator();

      Entry<Integer, Map<Object, Long>> actualEntry;
      Map<Object, Long> requestList;
      Integer addressIndex;
      if (iterator.hasNext()) {
         actualEntry = iterator.next();
         requestList = actualEntry.getValue();
         addressIndex = actualEntry.getKey();

         // Put objects of the first lisk into the fullList
         for (Entry<Object, Long> entry : requestList.entrySet()) {
            fullRequestList.put(entry.getKey(), new Pair<Long, Integer>(entry.getValue(), addressIndex));
         }
      }

      // For the following lists, when merging into the full list, has to
      // compare if its request has the highest remote access
      int conflictFailCnt = 0, conflictSuccCnt = 0, mergeCnt = 0;
      while (iterator.hasNext()) {
         actualEntry = iterator.next();
         requestList = actualEntry.getValue();
         addressIndex = actualEntry.getKey();

         for (Entry<Object, Long> entry : requestList.entrySet()) {
            Pair<Long, Integer> pair = fullRequestList.get(entry.getKey());
            if (pair == null) {
               fullRequestList.put(entry.getKey(), new Pair<Long, Integer>(entry.getValue(), addressIndex));
               ++mergeCnt;
            } else {
               if (pair.left < entry.getValue()) {
                  fullRequestList.put(entry.getKey(), new Pair<Long, Integer>(entry.getValue(), addressIndex));
                  ++conflictSuccCnt;
               } else {
                  ++conflictFailCnt;
               }
            }
         }
      }
      log.info("Merged:" + mergeCnt);
      log.info("Conflict but succeeded:" + conflictSuccCnt);
      log.info("Conflict but failed:" + conflictFailCnt);
      log.info("Size of fullrequestList: " + fullRequestList.size());

      return fullRequestList;
   }

   /**
    * create the object to move map, with the object and the new owner entries
    *
    * @param fullRequestList  the merged request list
    */
   private void populateObjectToMove(Map<Object, Pair<Long, Integer>> fullRequestList) {
      log.info("Generating final list");
      Map<Object, Long> localGetList = this.analyticsBean.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET);

      // !TODO Has to modify for better efficiency after debugging
      int failedConflict = 0, succeededConflict = 0;

      for (Entry<Object, Pair<Long, Integer>> entry : fullRequestList.entrySet()) {
         if (!localGetList.containsKey(entry.getKey())) {
            objectsToMove.put(entry.getKey(), entry.getValue().right);
         } else if (localGetList.get(entry.getKey()) < entry.getValue().left) {
            objectsToMove.put(entry.getKey(), entry.getValue().right);
            ++succeededConflict;
         } else {
            ++failedConflict;
         }
      }
      log.info("Succeeded conflict in final :" + succeededConflict);
      log.info("Failed conflict in final :" + failedConflict);
   }

   public void prePhaseTest(){
      log.debugf("Doing pre-phase testing!");
      log.debugf("SentObjectList size: %s", allSentObjects.size());
      log.debugf("Current round sent object size: %s", objectsToMove.size());

      for (Object key : objectsToMove.keySet()) {
         if (!allSentObjects.containsKey(key) && !dataContainer.containsKey(key)) {
            log.errorf("Trying to move a key that it does not have. Key is %s", key);
         }
      }
   }

   /**
    * Test if keys are moved out as expected 
    */
   public void postPhaseTest(){
      log.debug("Doing post-phase testing!");
      log.debugf("Size of DataContainer: %s", dataContainer.size());
      log.debugf("SentObjectsList size before merging current round: %s", allSentObjects.size());

      //Check if try to move some key twice
      log.debugf("Testing if keys are moved more than once");
      for(Entry<Object, Integer> entry : objectsToMove.entrySet()){
         Pair<Integer,Integer> temp = allSentObjects.get(entry.getKey());
         if(temp == null) {
            allSentObjects.put(entry.getKey(), new Pair<Integer, Integer>(entry.getValue(),1));
         } else if(entry.getValue() !=  temp.left.intValue()){
            ++temp.right;
            log.warnf("Moved %s more than one. Key moved %s times", entry.getKey(), temp.right);
         }
      }

      log.debugf("SentObjectsList size after merging current round: %s", allSentObjects.size());

      log.debugf("Testing if keys are moved correctly");
      int stillContainsCount = 0;
      for (Object key : objectsToMove.keySet()) {
         if (dataContainer.containsKey(key)) {
            log.errorf("Key %s not moved correctly", key);
            ++stillContainsCount;
         }
      }

      log.debugf("Test result: %s keys are not moved out correctly!", stillContainsCount);

      //Check if some keys are moved to some other places by using MLHash
      log.debugf("Testing keys new owner");
      int bfErrorCount = 0;
      for (Entry<Object, Integer> entry : objectsToMove.entrySet()) {
         Address expectedOwner = distributionManager.locate(entry.getKey()).get(0);
         Address newOwner = addressList.get(entry.getValue());
         if(!expectedOwner.equals(newOwner)){
            log.errorf("Key %s moved to a wrong owner. Expected owner is %s and the new owner is %s", entry.getKey(),
                       expectedOwner, newOwner);
            ++bfErrorCount;
         }
      }

      log.debugf("Test result: %s keys are in wrong place" ,bfErrorCount);
   }
}
