package org.infinispan.dataplacement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.infinispan.remoting.transport.Address;


public class TestWriter {
    private int sendCount = 0;
    private int receiveCount = 0;
    private int finalResultCounter = 0;
    private String outputFolder ="dpoutput/";
    private static TestWriter instance = null;
    
    private TestWriter(){
    	
    }
    
    public static TestWriter getInstance(){
    	if( instance == null)
    		instance = new TestWriter();
    	return instance;
    }
    
    public void write(boolean type, Address origin, Map<Object,Long> list){
    	String name;
		File f = new File(outputFolder);
		if(!f.exists()){   
		  f.mkdir();
		}
		
    	if(type == true){
    		name = "send-"+ (sendCount++) + "-" ;
    	}
    	else{
    		name = "receive-" + (receiveCount++) +"-" +origin.toString();
    	}
    	
		//BufferedReader br = new BufferedReader(fr);
		try {
			FileWriter fr = new FileWriter(outputFolder+name);
			fr.write(list.toString());
			fr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void writeResult(List<Pair<String, Integer>> list){
    	String name;
		
    	File f = new File(outputFolder);
		if(!f.exists()){   
		  f.mkdir();
		}
		
    	name = "result-" + (finalResultCounter++);

		try {
			FileWriter fr = new FileWriter(outputFolder+name);
			fr.write(list.toString());
			fr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
