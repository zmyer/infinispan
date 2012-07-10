package org.infinispan.dataplacement.lookup;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.dataplacement.c50.FileCropper;
import org.infinispan.dataplacement.c50.NameSplitter;
import org.infinispan.dataplacement.c50.TreeElement;
import org.infinispan.dataplacement.c50.TreeParser;

import com.clearspring.analytics.util.Pair;

public class ObjectLookUpper {

	BloomFilter bf;
	NameSplitter splitter = new NameSplitter();
	TreeParser treeParser;
	private List<String> rules;
	public static final String ML_FOLDER = "../ml";

	// TODO: confirm that this is an adequate value
	private static final double BF_FALSE_POSITIVE_PROB = 0.001;

	/**
	 * Constructor to build this object from data received through the network
	 * 
	 * @param bf
	 * @param treeList
	 */
	public ObjectLookUpper(BloomFilter bf,
			List<List<TreeElement>> treeList) {
		this.bf = bf;
		this.treeParser = new TreeParser(treeList);
	}

	/**
	 * Constructor to build this object from scratch, using a list of objects to
	 * populate its fields
	 * 
	 * @param toMoveObj
	 */
	public ObjectLookUpper(List<Pair<String, Integer>> toMoveObj) {
		this.writeObjToFiles(toMoveObj);
		try {
			this.populateRules();
			this.populateBloomFilter(toMoveObj);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private void writeObjToFiles(List<Pair<String, Integer>> toMoveObj) {
		List<Pair<String, Integer>> splittedKeys = NameSplitter
				.splitPairKeys(toMoveObj);
		FileWriter fw;
		try {
			fw = new FileWriter(ObjectLookUpper.ML_FOLDER + "/input.data");
			BufferedWriter bw = new BufferedWriter(fw);
			NameSplitter.writePairKeys(bw, splittedKeys);
			bw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void populateRules() throws IOException {
		String curDir = System.getProperty("user.dir");
		Process p = Runtime
				.getRuntime()
				.exec(ObjectLookUpper.ML_FOLDER + "/c5.0 -f" + ObjectLookUpper.ML_FOLDER + "/input");// >

		// Read result and store in a list
		BufferedReader input = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		String line;
		List<String> inputs = new ArrayList<String>();
		while ((line = input.readLine()) != null) {
			inputs.add(line);
		}
		input.close();

		this.rules = FileCropper.crop(inputs);

		// Create Tree
		this.treeParser.createTree(this.rules);
	}

	public void populateBloomFilter(List<Pair<String, Integer>> toMoveObj) {
		this.bf = new BloomFilter(toMoveObj.size(), ObjectLookUpper.BF_FALSE_POSITIVE_PROB);
		for (Pair<String, Integer> pair : toMoveObj) {
			this.bf.add(pair.left);
		}
	}

	public Integer query(String key) {
		if (!this.bf.contains(key))
			return null;
		else
			return this.treeParser.parseTree(NameSplitter.splitSingleKey(key));
	}

	public BloomFilter getBloomFilter() {
		return this.bf;
	}

	public List<List<TreeElement>> getTreeList() {
		return this.treeParser.getTreeList();
	}

	public String printRules() {
		String toReturn = "";
		for (String line : this.rules) {
			toReturn += line + "\n";
		}
		return toReturn;
	}
}
