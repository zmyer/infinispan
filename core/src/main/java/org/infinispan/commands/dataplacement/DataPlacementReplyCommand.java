package org.infinispan.commands.dataplacement;

import java.util.List;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.dataplacement.DataPlacementManager;
import org.infinispan.dataplacement.c50.TreeElement;
import org.infinispan.dataplacement.lookup.BloomFilter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class DataPlacementReplyCommand extends BaseRpcCommand {

	public static final byte COMMAND_ID = 121;
	private static final Log log = LogFactory.getLog(DataPlacementReplyCommand.class);
	private DataPlacementManager dataplacementMgr;
	private BloomFilter bloomFilter;
	private List<List<TreeElement>> treeList;
	private DATAPLACEPHASE phase;

	// public DataPlacementReplyCommand(){
	// super(null);
	// }
	public enum DATAPLACEPHASE {
		SETTING_PHASE, ACK_PHASE
	}

	public DataPlacementReplyCommand(String cacheName) {
		super(cacheName);
	}

	public void init(DataPlacementManager dpmgr) {
		this.dataplacementMgr = dpmgr;
	}

	public void setPhase(DATAPLACEPHASE phase) {
		this.phase = phase;
	}

	public void putBloomFilter(BloomFilter bf) {
		this.bloomFilter = bf;
	}

	public void putTreeElement(List<List<TreeElement>> tList) {
		this.treeList = tList;
	}

	@Override
	public Object perform(InvocationContext ctx) throws Throwable {
		if (this.phase == DATAPLACEPHASE.SETTING_PHASE) {
			this.dataplacementMgr.buildMLHashAndAck(this.getOrigin(), this.bloomFilter, this.treeList);
			DataPlacementReplyCommand.log.info("Get Look Upper from :" + this.getOrigin());
			// log.error("Size of bf:"+ bloomFilter.size());
		}
		else if (this.phase == DATAPLACEPHASE.ACK_PHASE) {
			DataPlacementReplyCommand.log.error("Get Ack from :" + this.getOrigin());
			this.dataplacementMgr.aggregateAck();
		} else {
			DataPlacementReplyCommand.log.error("Wrong phase!");
		}
		return null;
	}

	@Override
	public boolean isReturnValueExpected() {
		return false;
	}

	@Override
	public byte getCommandId() {
		return DataPlacementReplyCommand.COMMAND_ID;
	}

	@Override
	public Object[] getParameters() {
		return new Object[] { (byte) this.phase.ordinal(), this.bloomFilter, this.treeList };
	}

	@Override
	public void setParameters(int commandId, Object[] parameters) {
		this.phase = DATAPLACEPHASE.values()[(Byte) parameters[0]];
		this.bloomFilter = (BloomFilter) parameters[1];
		this.treeList = (List<List<TreeElement>>) parameters[2];
	}

}
