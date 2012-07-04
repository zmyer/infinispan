package org.infinispan.dataplacement.c50;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class FileCropper {

	/**
	 * @param args   path
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FileReader fr = null;
		boolean beforeTree = true, afterTree = false;
		try {
			fr = new FileReader(args[0]);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // jgpaiva: changed the readers
		BufferedReader br = new BufferedReader(fr);
		
		String outputFile = args[0]+"-cropped",
			    line;
		
		FileWriter fw;
		fw = new FileWriter(outputFile);
		BufferedWriter bw = new BufferedWriter(fw);
		
		while( (line = br.readLine())!= null){
			if( true == beforeTree ){
				if( line.startsWith("Decision tree:") == true ){
					beforeTree = false;
					br.readLine();
					continue;
				}
			}
			else if( false == afterTree){
				if(line.equals("")){
				  afterTree = true;
				  continue;
				}
			}
			
			if( false == beforeTree && false == afterTree)
				bw.write(line+"\n");
		}
		bw.close();
		fw.close();
		br.close();
		fr.close();
	}
	
	public void cropFileAndWrite(List<String> inputs, BufferedWriter bw) throws IOException{
		boolean beforeTree = true, afterTree = false;
		String line;
		for(int i =0; i< inputs.size(); ++i){
           line = inputs.get(i);
			if( true == beforeTree ){
				if( line.startsWith("Decision tree:") == true ){
					beforeTree = false;
					continue;
				}
			}
			else if( false == afterTree){
				if(line.startsWith("Evaluation")){
				  afterTree = true;
				  continue;
				}
			}
			
			if( false == beforeTree && false == afterTree && !line.isEmpty())
				bw.write(line+"\n");
		}
	}

}
