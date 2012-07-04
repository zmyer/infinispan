package org.infinispan.commands.dataplacement;

import java.util.List;

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
	
//	public DataPlacementReplyCommand(){
//		super(null);
//	}
	
	public DataPlacementReplyCommand(String cacheName){
		super(cacheName);
	}
	
	public void init(DataPlacementManager dpmgr){
		dataplacementMgr = dpmgr;
	}
	
	public void putBloomFilter(SimpleBloomFilter bf){
		bloomFilter = bf;
	}
	
	public void putTreeElement(List<List<TreeElement>> tList) {
		treeList = tList;
	}
	
	@Override
	public Object perform(InvocationContext ctx) throws Throwable {
		dataplacementMgr.setLookUpper(bloomFilter, treeList);
		log.error("Has received reply!!!!");
		//log.error("Size of bf:"+ bloomFilter.size());
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
		return new Object[] { bloomFilter, treeList };
	}

	@Override
	public void setParameters(int commandId, Object[] parameters) {
		bloomFilter = (SimpleBloomFilter<String>)parameters[0];
		treeList = (List<List<TreeElement>>) parameters[1];
	}



}
