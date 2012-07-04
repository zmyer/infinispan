package org.infinispan.dataplacement.lookup;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

public class Reader {

	int mode;
	File file;
	Integer nodeNumber;

	BufferedReader br;

	static Reader instance = null;

	public static Reader getInstance() {
		if (instance == null) {
			instance = new Reader();
		}
		return instance;
	}

	private Reader() {

	}


	public  void StartReading(String path) throws FileNotFoundException {
		FileReader fr = new FileReader(path); // jgpaiva: changed the readers
		this.br = new BufferedReader(fr);
//		Integer nodeNumber = new Integer(file.getName().substring(14, 15));
	}

	public int GetElements(List<String> keys)
			throws NumberFormatException, IOException {
		String str;
		String element;
		int result;

		StringTokenizer strToken = null;

		if ((str = br.readLine()) != null) {
			if (str.startsWith("Remote")){
				if(str.startsWith("RemoteTopGets"))
				  result = 1;
				else
				  result = 3;
			}
			else{
				if(str.startsWith("LocalTopGets"))
				  result = 2;
				else
				  result = 4;
			}
			
			if (str.endsWith("{}") == false) {
				strToken = new StringTokenizer(str, "{}, "); // jgpaiva: changed
				// the tokens to
				// tokenize on
				strToken.nextToken(); // jgpaiva: skip first token, is name of
				// field;

				while (strToken.hasMoreElements()) {

					element = strToken.nextToken();
					String[] tmpArray = element.split("=");
					keys.add(tmpArray[0]);
					//values.add(new Integer(tmpArray[1]));
				}
			}
			return result;
		}

		return 0;
	}
	
	public void StopReading(){
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// void writeOutput(String fileName, int nodeNumber) throws IOException{
	// if(mode == 0)
	// sta.writeOutput(fileName, nodeNumber);
	// else
	// opt.writeOutput(fileName);
	// }

}
