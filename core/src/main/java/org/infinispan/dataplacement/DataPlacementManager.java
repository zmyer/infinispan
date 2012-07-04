package org.infinispan.dataplacement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.dataplacement.DataPlacementReplyCommand;
import org.infinispan.commands.dataplacement.DataPlacementRequestCommand;
import org.infinispan.dataplacement.c50.TreeElement;
import org.infinispan.dataplacement.lookup.ObjectLookUpper;
import org.infinispan.dataplacement.lookup.SimpleBloomFilter;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.Util;
import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.Cache;

import com.clearspring.analytics.util.Pair;



public class DataPlacementManager {
	
	
	private static final Log log = LogFactory.getLog(DataPlacementManager.class);
	private RpcManager rpcManager;
	private DistributionManager distributionManager;
	private CommandsFactory commandsFactory;
	private Timer timer;
	private StreamLibContainer analyticsBean;
	private ObjectLookUpper lookUpper = new ObjectLookUpper();
	private TestWriter writer = new TestWriter();
	private CacheViewsManager cacheViewsManager;
	private Cache cache;
	
	private Integer requestRound = 0,
			        replyRound = 0;
	
	private List<Address> addressList = new ArrayList<Address>();
	List<Pair<Integer,Map<Object,Long>>> objectRequestList = new ArrayList<Pair<Integer,Map<Object,Long>>>();

	public DataPlacementManager(){
	      this.analyticsBean = StreamLibContainer.getInstance();
	}
	
   @Inject
   public void inject(CommandsFactory commandsFactory, DistributionManager distributionManager, RpcManager rpcManager, 
		   CacheViewsManager cacheViewsManager, Cache cache) {
      this.commandsFactory = commandsFactory;
      this.distributionManager = distributionManager;
      this.rpcManager = rpcManager;

      this.cacheViewsManager = cacheViewsManager;
      this.cache = cache;
	  timer = new Timer();
	  timer.schedule(new DataPlaceRequestTask(), 600000, 300000);
   }
	
	public void sendRequestToAll(){
		
		Map<Object, Long> remoteGet =  analyticsBean.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET,analyticsBean.getCapacity());
				//remotePut = analyticsBean.getTopKFrom(AnalyticsBean.Stat.REMOTE_PUT,analyticsBean.getCapacity());
		
		//Only send statistics if there are enough objects
		if(remoteGet.size() >=800){
			Map<Address, Map<Object,Long>> remoteTopLists = sortObjectsByOwner(remoteGet);
				
			for(Entry<Address, Map<Object,Long>> entry : remoteTopLists.entrySet()){
				sendRequest(entry.getKey(), entry.getValue());
			}
			Set<Address> addresses = distributionManager.getConsistentHash().getCaches();
			for(Address add : addresses){
				if(remoteTopLists.containsKey(add) == false)
					sendRequest(add,new HashMap<Object,Long>());
			}
			
			++requestRound;
		}
	}
	
	private void sendRequest(Address owner, Map<Object,Long> remoteTopList){
		
		DataPlacementRequestCommand command = commandsFactory.buildDataPlacementRequestCommand();
        command.init(this, distributionManager);
        log.error("Putting Message with size "+ remoteTopList.size());
        command.putRemoteList(remoteTopList, requestRound);
        if( !rpcManager.getAddress().toString().equals(owner) ){
           try {
        	  rpcManager.invokeRemotely(Collections.singleton(owner), command, false);
        	  log.error("Message sent!");
        	  writer.write(true, null, remoteTopList);
           } catch (Throwable throwable) {
        	   log.error(throwable);
           }
        }
        else{
        	log.error("Message will not be sent to myself!");
        }
	}
	
	private Map<Address,Map<Object,Long>> sortObjectsByOwner(Map<Object, Long> remoteGet) {
	    Map<Address,Map<Object,Long>> objectLists = new HashMap<Address,Map<Object, Long>>();	
        Map<Object, List<Address>> mappedObjects = distributionManager.locateAll(remoteGet.keySet(), 1);
		
        Address addr = null;
        Object key = null;
        
        for(Entry<Object, Long> entry : remoteGet.entrySet()){
        	key = entry.getKey();
        	addr = mappedObjects.get(key).get(0);
        	
        	if(!objectLists.containsKey(addr)){
        		objectLists.put(addr, new HashMap<Object,Long>());
        	}
        	objectLists.get(addr).put(entry.getKey(), entry.getValue());
        }
	    	
		return objectLists;
	}
	

	//Aggregate!
	public void aggregateRequests(Address sender, Map<Object,Long> objectRequest, Integer roundID){
		log.error("Aggregating request!");
		try{
			
			//Convert the sender address to id to reduce space
			if( addressList.size() != distributionManager.getConsistentHash().getCaches().size()){
			   Object[] addArray = distributionManager.getConsistentHash().getCaches().toArray();
			   addressList.clear();
			   for(int i = 0; i< addArray.length; ++i){	
			 	 addressList.add((Address)addArray[i]);
			 	 log.error("Current Address List: ["+ i + "] " + addArray[i]);
			   }
			}
			
			Integer senderID = 	addressList.indexOf(sender);
		    log.error("Getting message of round "+ roundID+ "from node"+ sender);
			
			if(roundID == replyRound)
			 objectRequestList.add(new Pair<Integer, Map<Object,Long>>(senderID, objectRequest));
	        
			if(addressList.size() - objectRequestList.size() == 1){
				log.error("Everyone has sent request!!! "+ addressList.size() +" in total!");
				writer.write(false, sender, objectRequest);
				
				Map<Object, Pair<Long, Integer>> fullRequestList = compactRequestList();
				List<Pair<String, Integer>>  finalResultList = generateFinalList(fullRequestList);
				writer.writeResult(finalResultList);
				
				lookUpper.populateAll(finalResultList);
				
				log.error("Rules:");
				log.error(lookUpper.getRules());
				
				sendResult(lookUpper.getBloomFilter(), lookUpper.getTreeList());
				objectRequestList.clear();
				++replyRound;
			}
			else
			 log.error("Gathering request... has received from" + objectRequestList.size() +" nodes");
			}
		catch(Exception e){
			log.error(e);
		}
	}
	
	/*
	 * Merge the request lists from all other nodes into a single request list
	 */
	public Map<Object, Pair<Long, Integer>> compactRequestList(){
        Map<Object, Pair<Long, Integer>> fullRequestList = new HashMap<Object,Pair<Long, Integer>>();
		
		Map<Object,Long> requestList = objectRequestList.get(0).right;
		Integer addressIndex = objectRequestList.get(0).left;
		
		//Put objects of the first lisk into the fullList
		for(Entry<Object,Long> entry : requestList.entrySet()){
			fullRequestList.put(entry.getKey(), new Pair<Long,Integer>(entry.getValue(), addressIndex));
		}
			
		//For the following lists, when merging into the full list, has to compare if its request has the 
		// highest remote access
		int conflictFailCnt = 0, conflictSuccCnt = 0, mergeCnt = 0;
		for(int i =1 ; i< objectRequestList.size(); ++i){
			requestList = objectRequestList.get(i).right;
			addressIndex = objectRequestList.get(i).left;
			for(Entry<Object,Long> entry : requestList.entrySet()){
				Pair<Long,Integer> pair = fullRequestList.get(entry.getKey());
				if(pair == null){
					fullRequestList.put(entry.getKey(), new Pair<Long,Integer>(entry.getValue(), addressIndex));
				    ++mergeCnt;
				}
				else{
					if(pair.left < entry.getValue()){
					 fullRequestList.put(entry.getKey(), new Pair<Long,Integer>(entry.getValue(), addressIndex));
					  ++conflictSuccCnt;
					 //log.error("Conflicting in merging.. But succeeded " + entry.getValue() + " :"+ pair.left);	
					}
					else{
					  ++conflictFailCnt;
					   //log.error("Conflicting in merging.. But failed " + entry.getValue() + " :"+ pair.left);	
					}
				}
			}
		}
		log.error("Merged:"+ mergeCnt);
		log.error("Conflict but succeeded:"+ conflictSuccCnt);
		log.error("Conflict but failed:"+ conflictFailCnt);
		log.error("Size of fullrequestList: "+ fullRequestList.size());
		
		return fullRequestList;
	}
	
	/*
	 * Compare the remote access of every entry in the full request list and return the 
	 * final resultList
	 */
	public List<Pair<String, Integer>> generateFinalList(Map<Object, Pair<Long, Integer>>  fullRequestList){
		List<Pair<String, Integer>> resultList = new ArrayList<Pair<String,Integer>>();
		Map<Object,Long> localGetList = analyticsBean.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET,analyticsBean.getCapacity());
				//localPutList = analyticsBean.getTopKFrom(AnalyticsBean.Stat.LOCAL_PUT, analyticsBean.getCapacity());
		
		
		//!TODO Has to modify back for better efficiency
		for(Entry<Object, Pair<Long, Integer>> entry : fullRequestList.entrySet() ){
			if(localGetList.containsKey(entry.getKey()) == false){
				resultList.add(new Pair<String, Integer>(entry.getKey().toString(), entry.getValue().right));
			}
			else if( localGetList.get(entry.getKey()) < entry.getValue().left){
				resultList.add(new Pair<String, Integer>(entry.getKey().toString(), entry.getValue().right));
				log.error("Conflicting in final.. But succeeded " + entry.getValue().left + " :"+ localGetList.get(entry.getKey()));
			}
			else
				log.error("Conflicting in final.. And failed");
		}
		
		return resultList;
	}

	
	public void sendResult(SimpleBloomFilter simpleBloomFilter, List<List<TreeElement>> treeList){
        DataPlacementReplyCommand command = commandsFactory.buildDataPlacementReplyCommand();
        command.init(this);
        command.putBloomFilter(simpleBloomFilter);
        command.putTreeElement(treeList);
        log.error("Trying to send result(really!)");
        
        cacheViewsManager.handleRequestMoveKeys(cache.getName());
        
        try {
        	  rpcManager.invokeRemotely(null, command, false);
           } catch (Throwable throwable) {
              log.error(throwable);
         }
	}
	
	public void setLookUpper(SimpleBloomFilter bf, List<List<TreeElement>> treeList){
		lookUpper.setBloomFilter(bf);
		lookUpper.setTreeElement(treeList);
		log.error("Look Upper Set!");
	}
	
	class DataPlaceRequestTask extends TimerTask {
	    public void run() {
	      sendRequestToAll();
	      log.error("Timer Runned Once!");
	    }
	}
}
