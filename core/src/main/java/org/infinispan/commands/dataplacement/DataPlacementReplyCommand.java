package org.infinispan.commands.dataplacement;

import java.util.List;

import org.infinispan.commands.control.CacheViewControlCommand.Type;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.dataplacement.DataPlacementManager;
import org.infinispan.dataplacement.c50.TreeElement;
import org.infinispan.dataplacement.lookup.SimpleBloomFilter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class DataPlacementReplyCommand extends BaseRpcCommand{
	
	public static final byte COMMAND_ID = 121;
	private static final Log log = LogFactory.getLog(DataPlacementReplyCommand.class);
	private DataPlacementManager dataplacementMgr;
	private SimpleBloomFilter<String> bloomFilter;
	private List<List<TreeElement>> treeList;
	private DATAPLACEPHASE phase;
	
//	public DataPlacementReplyCommand(){
//		super(null);
//	}
	public enum DATAPLACEPHASE{
		SETTING_PHASE, ACK_PHASE
	}
	
	public DataPlacementReplyCommand(String cacheName){
		super(cacheName);
	}
	
	public void init(DataPlacementManager dpmgr){
		dataplacementMgr = dpmgr;
	}
	
	public void setPhase(DATAPLACEPHASE phase){
	    this.phase = phase;	
	}
	
	public void putBloomFilter(SimpleBloomFilter bf){
		bloomFilter = bf;
	}
	
	public void putTreeElement(List<List<TreeElement>> tList) {
		treeList = tList;
	}
	
	@Override
	public Object perform(InvocationContext ctx) throws Throwable {
		if(phase == DATAPLACEPHASE.SETTING_PHASE){
			dataplacementMgr.setLookUpper(getOrigin(), bloomFilter, treeList);
			log.info("Get Look Upper from :"+getOrigin());
			//log.error("Size of bf:"+ bloomFilter.size());
		}
		else if ( phase == DATAPLACEPHASE.ACK_PHASE ){
			log.error("Get Ack from :" + getOrigin());
			dataplacementMgr.aggregateAck();
		}
		else
			log.error("Wrong phase!");
		return null;
	}
	
    @Override
    public boolean isReturnValueExpected() {
        return false;
    }

	@Override
	public byte getCommandId() {
		return COMMAND_ID;
	}

	@Override
	public Object[] getParameters() {
		return new Object[] { (byte) phase.ordinal(), bloomFilter, treeList };
	}

	@Override
	public void setParameters(int commandId, Object[] parameters) {
		phase = DATAPLACEPHASE.values()[(Byte) parameters[0]];
		bloomFilter = (SimpleBloomFilter<String>)parameters[1];
		treeList = (List<List<TreeElement>>) parameters[2];
	}



}
