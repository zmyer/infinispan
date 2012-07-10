package org.infinispan.commands.dataplacement;

import java.util.Map;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.dataplacement.DataPlacementManager;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class DataPlacementRequestCommand extends BaseRpcCommand {

	public static final byte COMMAND_ID = 120;
	private static final Log log = LogFactory.getLog(DataPlacementRequestCommand.class);
	// cache components
	private Integer rountID;
	private DistributionManager distributionManager;
	private DataPlacementManager dataPlacementManager;

	Map<Object, Long> remoteTopObjects;

	// public DataPlacementRequestCommand() {
	// super(null);
	// }

	public DataPlacementRequestCommand(String cacheName) {
		super(cacheName);
	}

	public void init(DataPlacementManager dataPlacementManager,
			DistributionManager distributionManager) {
		this.dataPlacementManager = dataPlacementManager;
		this.distributionManager = distributionManager;
	}

	public void putRemoteList(Map<Object, Long> objectList, int roundID) {
		this.remoteTopObjects = objectList;
		this.rountID = roundID;
		// log.error("Message Put Inside! :" + remoteTopObjects.size());
	}

	@Override
	public boolean isReturnValueExpected() {
		return false;
	}

	@Override
	public Object perform(InvocationContext ctx) throws Throwable {
		// log.error("Message Received " + remoteTopObjects.size() +
		// " of round "+rountID);
		this.dataPlacementManager.aggregateRequests(this.getOrigin(), this.remoteTopObjects,
				this.rountID);
		return null;
	}

	@Override
	public byte getCommandId() {
		return DataPlacementRequestCommand.COMMAND_ID;
	}

	@Override
	public Object[] getParameters() {
		return new Object[] { this.remoteTopObjects, this.rountID };
	}

	@Override
	public void setParameters(int commandId, Object[] parameters) {
		// cacheName = (String) parameters[0];
		this.remoteTopObjects = (Map<Object, Long>) parameters[0];
		this.rountID = (Integer) parameters[1];
	}
}
