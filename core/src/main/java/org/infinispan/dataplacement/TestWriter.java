package org.infinispan.dataplacement;

import org.infinispan.remoting.transport.Address;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * //TODO document this
 *
 * @author Zhongmiao Li
 * @author João Paiva
 * @since 5.2
 */
public class TestWriter {
   private int sendCount = 0;
   private int receiveCount = 0;
   private static final String OUTPUT_FOLDER = "dpoutput/";
   private static TestWriter instance = null;

   private TestWriter() {}

   public synchronized static TestWriter getInstance(){
      if (instance == null) {
         instance = new TestWriter();
      }
      return instance;
   }

   public void write(boolean send, Address origin, Map<Object,Long> list){
      String name;
      File f = new File(OUTPUT_FOLDER);
      if(!f.exists()){
         if (!f.mkdir()) {
            return;
         }
      }

      if (send) {
         name = "send-"+ (sendCount++) + "-" ;
      } else {
         name = "receive-" + (receiveCount++) + "-" + origin.toString();
      }

      //BufferedReader br = new BufferedReader(fr);
      try {
         FileWriter fr = new FileWriter(OUTPUT_FOLDER + name);
         fr.write(list.toString());
         fr.close();
      } catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
}
