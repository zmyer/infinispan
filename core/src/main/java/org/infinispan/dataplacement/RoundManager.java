package org.infinispan.dataplacement;

/**
 * Manages the round Id, blocks commands from round ahead of time and data placement request if another request is 
 * in progress
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class RoundManager {

   private long currentRoundId;

   //in milliseconds
   private long coolDownTime;
   //after this time, a next round can start
   private long nextRoundTimestamp;

   private boolean roundInProgress;

   private boolean enabled = false;

   public RoundManager(long coolDownTime) {
      this.coolDownTime = coolDownTime;
      roundInProgress = false;
      updateNextRoundTimestamp();
   }

   /**
    * enables the data placement optimization
    */
   public final synchronized void enable() {
      enabled = true;
   }

   /**
    * returns the current round Id
    *
    * @return  the current round Id
    */
   public final synchronized long getCurrentRoundId() {
      return currentRoundId;
   }

   /**
    * returns a new round Id. this is invoked by the coordinator before start a new round´
    *
    * @return           a new round Id
    * @throws Exception if the last request happened recently or another request is in progress
    */
   public final synchronized long getNewRoundId() throws Exception {
      if (!enabled) {
         throw new Exception("Data Placement optimization not enabled");
      }

      if (System.currentTimeMillis() < nextRoundTimestamp) {
         throw new Exception("Cannot start the next round. The last round happened recently");
      }

      if (roundInProgress) {
         throw new Exception("Cannot start the next round. Another round is in progress");
      }

      updateNextRoundTimestamp();
      roundInProgress = true;
      return ++currentRoundId;
   }

   /**
    * it blocks the current thread until the current round is higher or equals to the round id
    *
    * @param roundId    the round id
    * @throws Exception if interrupted while waiting or if the component is not enabled
    */
   public final synchronized void ensure(long roundId) throws Exception {
      if (!enabled) {
         throw new Exception("Data placement optimization not enabled");
      }
      while (roundId > currentRoundId) {
         wait();
      }
   }

   /**
    * invoked in all members when a new round starts
    * @param roundId the new round id
    */
   public final synchronized void startNewRound(long roundId) {
      currentRoundId = roundId;
      roundInProgress = true;
      notifyAll();
   }

   /**
    * mark a current round as finished
    */
   public final synchronized void markRoundFinished() {
      roundInProgress = false;
   }

   /**
    * sets the new cool down time. it only takes effect after the next round
    *
    * @param coolDownTime  the new cool down time in milliseconds
    */
   public final synchronized void setCoolDownTime(long coolDownTime) {
      this.coolDownTime = coolDownTime;
   }

   /**
    * updates the cool down time before start a new request
    */
   private void updateNextRoundTimestamp() {
      nextRoundTimestamp = System.currentTimeMillis() + coolDownTime;
   }
}
