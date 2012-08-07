package org.infinispan.commands.remote;

import org.infinispan.context.InvocationContext;
import org.infinispan.dataplacement.DataPlacementManager;

import java.util.Map;

/**
 * The command used to send information among nodes
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementCommand extends BaseRpcCommand {

   public static final short COMMAND_ID = 102;

   public static enum Type {
      /**
       * send to the coordinator to request the start of the algorithm
       */
      DATA_PLACEMENT_REQUEST,

      /**
       * coordinator broadcast this to start the algorithm
       */
      DATA_PLACEMENT_START,

      /**
       * contains the remote top list
       */
      REMOTE_TOP_LIST_PHASE,

      /**
       * contains the object lookup
       */
      OBJECT_LOOKUP_PHASE,

      /**
       * contains the final ACK to start the state transfer
       */
      ACK_COORDINATOR_PHASE,

      /**
       * sets the new cool down period
       */
      SET_COOL_DOWN_TIME
   }

   private DataPlacementManager dataPlacementManager;

   //message data
   private Type type;
   private long roundId;
   private int coolDownTime;
   private Map<Object, Long> remoteTopList;
   private Object[] objectLookupParameters;


   public DataPlacementCommand(String cacheName, Type type, long roundId) {
      super(cacheName);
      this.type = type;
      this.roundId = roundId;
   }

   public DataPlacementCommand(String cacheName) {
      super(cacheName);
   }

   public final void initialize(DataPlacementManager dataPlacementManager) {
      this.dataPlacementManager = dataPlacementManager;
   }

   public void setRemoteTopList(Map<Object, Long> remoteTopList) {
      this.remoteTopList = remoteTopList;
   }

   public void setObjectLookup(Object[] objectLookupParameters) {
      this.objectLookupParameters = objectLookupParameters;
   }

   public void setCoolDownTime(int coolDownTime) {
      this.coolDownTime = coolDownTime;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      switch (type) {
         case DATA_PLACEMENT_REQUEST:
            dataPlacementManager.dataPlacementRequest();
            break;
         case DATA_PLACEMENT_START:
            dataPlacementManager.startDataPlacement(roundId);
            break;
         case REMOTE_TOP_LIST_PHASE:
            dataPlacementManager.addRequest(getOrigin(), remoteTopList, roundId);
            break;
         case OBJECT_LOOKUP_PHASE:
            dataPlacementManager.addObjectLookup(getOrigin(), objectLookupParameters, roundId);
            break;
         case ACK_COORDINATOR_PHASE:
            dataPlacementManager.addAck(roundId);
            break;
         case SET_COOL_DOWN_TIME:
            dataPlacementManager.internalSetCoolDownTime(coolDownTime);
            break;
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      switch (type) {
         case DATA_PLACEMENT_REQUEST:
            return new Object[] {(byte) type.ordinal()};
         case DATA_PLACEMENT_START:
         case ACK_COORDINATOR_PHASE:
            return new Object[] {(byte) type.ordinal(), roundId};
         case REMOTE_TOP_LIST_PHASE:
            return new Object[] {(byte) type.ordinal(), roundId, remoteTopList};
         case OBJECT_LOOKUP_PHASE:
            Object[] retVal = new Object[2 + objectLookupParameters.length];
            retVal[0] = (byte) type.ordinal();
            retVal[1] = roundId;
            System.arraycopy(objectLookupParameters, 0, retVal, 2, objectLookupParameters.length);
            return retVal;
         case SET_COOL_DOWN_TIME:
            return new Object[] {(byte) type.ordinal(), coolDownTime};
      }
      throw new IllegalStateException("This should never happen!");
   }

   @SuppressWarnings("unchecked")
   @Override
   public void setParameters(int commandId, Object[] parameters) {
      type = Type.values()[(Byte)parameters[0]];

      switch (type) {
         case DATA_PLACEMENT_START:
         case ACK_COORDINATOR_PHASE:
            roundId = (Long) parameters[1];
            break;
         case REMOTE_TOP_LIST_PHASE:
            remoteTopList = (Map<Object, Long>) parameters[1];
            break;
         case OBJECT_LOOKUP_PHASE:
            roundId = (Long) parameters[1];
            objectLookupParameters = new Object[parameters.length - 2];
            System.arraycopy(parameters, 2, objectLookupParameters, 0, objectLookupParameters.length);
            break;
         case SET_COOL_DOWN_TIME:
            coolDownTime = (Integer) parameters[1];
      }
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }
}
