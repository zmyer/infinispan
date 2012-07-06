package org.infinispan.commands.dataplacement;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.context.InvocationContext;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.dataplacement.DataPlacementManager;
import org.infinispan.factories.annotations.Inject;

public class DataPlacementRequestCommand extends BaseRpcCommand {

	public static final byte COMMAND_ID = 120;
	private static final Log log = LogFactory.getLog(DataPlacementRequestCommand.class);
    // cache components
	private Integer rountID;
    private DistributionManager distributionManager;
    private  DataPlacementManager dataPlacementManager;
    
    Map<Object,Long> remoteTopObjects;
 
//	public DataPlacementRequestCommand() {
//		super(null);
//	}
	
	public DataPlacementRequestCommand(String cacheName){
		super(cacheName);
	}

	 public void init(DataPlacementManager dataPlacementManager , DistributionManager distributionManager) {
		this.dataPlacementManager = dataPlacementManager;
		 this.distributionManager = distributionManager;
	 }
	 
	public void putRemoteList(Map<Object,Long> objectList, int roundID){
		remoteTopObjects = objectList;
		this.rountID = roundID;
		//log.error("Message Put Inside! :" + remoteTopObjects.size());
	}

    @Override
    public boolean isReturnValueExpected() {
        return false;
    }
	
	
	@Override
	public Object perform(InvocationContext ctx) throws Throwable {
        //log.error("Message Received " + remoteTopObjects.size() + " of round "+rountID);
        dataPlacementManager.aggregateRequests(getOrigin(), remoteTopObjects, rountID);
        return null;
	}

	@Override
	public byte getCommandId() {
		return COMMAND_ID;
	}

	@Override
	public Object[] getParameters() {
		return new Object[] { remoteTopObjects, rountID };
	}

	@Override
	public void setParameters(int commandId, Object[] parameters) {
		//cacheName = (String) parameters[0];
		remoteTopObjects = (Map<Object,Long>) parameters[0];
		rountID = (Integer) parameters[1];
	}
}
