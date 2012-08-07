package org.infinispan.dataplacement;

import org.infinispan.container.DataContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DataPlacementConsistentHashing;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Manages all the remote access and creates the request list to send to each other member
 *
 * @author Zhongmiao Li
 * @author João Paiva
 * @author Pedro Ruivo
 * @since 5.2
 */
public class RemoteAccessesManager {
   private static final Log log = LogFactory.getLog(RemoteAccessesManager.class);

   private final Set<Object> movedInList;
   private final Set<Object> requestSentList;

   private final DistributionManager distributionManager;
   private final DataContainer dataContainer;

   private final Map<Address, Map<Object, Long>> remoteAccessPerMember;
   private final StreamLibContainer analyticsBean;

   private final TestWriter writer;

   public RemoteAccessesManager(DistributionManager distributionManager, DataContainer dataContainer) {
      this.distributionManager = distributionManager;
      this.dataContainer = dataContainer;

      analyticsBean = StreamLibContainer.getInstance();
      writer = TestWriter.getInstance();
      remoteAccessPerMember = new HashMap<Address, Map<Object, Long>>();
      movedInList = new HashSet<Object>();
      requestSentList = new HashSet<Object>();
   }

   /**
    * reset the state (before each round)
    */
   public final synchronized void resetState() {
      remoteAccessPerMember.clear();
      requestSentList.clear();
   }

   //TODO I'm still just sending the remote top get! If remote top put is really need, could just add a new variable inside DataPlacementRequestCommand

   /**
    * calculates the remote request list to send for each member
    */
   public synchronized final void calculateRemoteAccessesPerMember(){
      if (!remoteAccessPerMember.isEmpty()) {
         return;
      }

      Map<Object, Long> remoteGet = analyticsBean.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET);

      log.info("Size of Remote Get is :" + remoteGet.size());

      // Only send statistics if there are enough objects
      if (remoteGet.size() >= analyticsBean.getCapacity() * 0.8) {
         Map<Object, Long> localGet = getStaticsForMovedInObjects();
         log.info("Size of movedin objects:"+localGet.size());

         Map<Address, Map<Object, Long>> tmpList = sortObjectsByOwner(remoteGet,true);

         for(Entry<Address,Map<Object, Long>> map : tmpList.entrySet()){
            log.info("Sorting remote list:"+map.getKey() +": Size of list"+ map.getValue().size());
         }

         remoteGet.putAll(localGet);
         requestSentList.addAll(remoteGet.keySet());
         log.info("Merged list size:"+remoteGet.size());
         remoteAccessPerMember.putAll(sortObjectsByOwner(remoteGet,true));
      }
   }

   /**
    * returns the remote request list to send for the member
    * @param member  the member to send the list
    * @return        the request list, object and number of accesses, or null if it has no accesses to that member
    */
   public synchronized final Map<Object, Long> getRemoteListFor(Address member) {
      Map<Object, Long> request = remoteAccessPerMember.get(member);
      writer.write(true, null, request);
      return request;
   }

   /**
    * Get the access number for all moved in keys.
    * If they are in the top local list, their access number is just what is in the top local list;
    * Otherwise, estimate their access numbers as the minimum value of the access number of any movedin key 
    * in the top local list.
    *
    * @return  the access number for all moved keys
    */
   private Map<Object, Long> getStaticsForMovedInObjects() {
      Map<Object, Long> localGetRealStatics = this.analyticsBean.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET);

      Map<Object, Long> localGetEstimatedStatics = new HashMap<Object,Long>();
      List<Object> tempList = new ArrayList<Object>();
      long minAccess = Long.MAX_VALUE;

      for(Object key: movedInList){
         Long accessNum = localGetRealStatics.get(key);

         //If the movedin key is in the local top get list, then put it in
         // the list and also update the access number.
         if( accessNum != null){
            localGetEstimatedStatics.put(key, accessNum);
            if(accessNum < minAccess)
               minAccess = accessNum;
         } else {
            tempList.add(key);
         }
      }

      //If not found any in the local top list, set minaccess to 1.
      if(minAccess == Long.MAX_VALUE)
         minAccess = 1;

      log.info("Movedin Object list size :"+movedInList.size());
      log.info("Min Access Num is:"+minAccess);
      log.info("Number of objects moved in but not in local top list:"+tempList.size());

      for(Object key : tempList){
         localGetEstimatedStatics.put(key, minAccess);
      }

      return localGetEstimatedStatics;
   }

   /**
    * sort the keys and access sorted by owner
    *
    * @param remoteGet        the remote accesses
    * @param useDefaultHash   true if it should use the owner of the default consistent hash
    * @return                 the map between owner and each object and number of access 
    */
   private Map<Address, Map<Object, Long>> sortObjectsByOwner(Map<Object, Long> remoteGet, boolean useDefaultHash) {
      Map<Address, Map<Object, Long>> objectLists = new HashMap<Address, Map<Object, Long>>();
      Map<Object, List<Address>> mappedObjects = getConsistentHashingFunction(useDefaultHash).locateAll(remoteGet.keySet(), 1);

      Address address;
      Object key;

      for (Entry<Object, Long> entry : remoteGet.entrySet()) {
         key = entry.getKey();
         address = mappedObjects.get(key).get(0);

         if (!objectLists.containsKey(address)) {
            objectLists.put(address, new HashMap<Object, Long>());
         }
         objectLists.get(address).put(entry.getKey(), entry.getValue());
      }

      log.info("Own number of movedin object after sorting:"+objectLists.size());

      log.info("MLHASH: List Sorting: Number of owner"+ objectLists.size());
      for(Entry<Address,Map<Object, Long>> map : objectLists.entrySet()){
         log.info(map.getKey() +": Size of list"+ map.getValue().size());
      }

      return objectLists;
   }

   /**
    * Try to add keys that was requested and have been moved in in the last round 
    */
   public void postPhaseTest(){

      log.info("Request List Size: "+requestSentList.size());
      log.info("Movedin List Size: "+ movedInList.size());

      log.info("Adding moved-in list");

      log.info("Moved In List size before deleting is:"+movedInList.size());

      Set<Object> tempSet = new HashSet<Object>();
      //Delete keys that are moved out in this round. Use temp list to avoid concurrent update..
      for (Object key : movedInList){
         if(dataContainer.containsKey(key)){
            tempSet.add(key);
         }
      }
      movedInList.clear();
      movedInList.addAll(tempSet);

      log.info("Moved In List size after deleting is:"+ movedInList.size());

      //Add moved-in keys into the list
      for (Object key : requestSentList){
         if(dataContainer.containsKey(key)){
            movedInList.add(key);
         }
      }
      log.info("Moved In List size after merge is:"+movedInList.size());

      //Clean up- the local and remote top get list 
      analyticsBean.resetAll();
      log.info(analyticsBean.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET).size());

      log.info(analyticsBean.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET).size());

      log.info("Request Sent List:"+requestSentList.toString());
   }

   /**
    * returns the actual consistent hashing
    *
    * @param useDefaultHash   true to return the default consistent hash
    * @return                 the actual consistent hashing
    */
   private ConsistentHash getConsistentHashingFunction(boolean useDefaultHash) {
      ConsistentHash hash = this.distributionManager.getConsistentHash();
      if (hash instanceof DataPlacementConsistentHashing){
         if(useDefaultHash){
            log.info("Returning default hash of MLHash");
            return ((DataPlacementConsistentHashing) hash).getDefaultHash();
         }
         else{
            log.info("Returning MLHash");
            return hash;
         }
      }
      else{
         if(useDefaultHash){
            log.info("Returning default hash");
         }
         else{
            log.info("No MLHashing!");
         }
         log.info("Returning normal hash");
         return hash;
      }
   }
}
