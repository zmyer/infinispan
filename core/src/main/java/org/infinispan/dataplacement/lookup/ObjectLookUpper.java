package org.infinispan.dataplacement.lookup;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.dataplacement.Pair;
import org.infinispan.dataplacement.c50.FileCropper;
import org.infinispan.dataplacement.c50.NameSplitter;
import org.infinispan.dataplacement.c50.TreeElement;
import org.infinispan.dataplacement.c50.TreeParser;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class ObjectLookUpper {

	private static final Log log = LogFactory
			.getLog(ObjectLookUpper.class);
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
		this.treeParser = new TreeParser(treeList, true);
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
			ObjectLookUpper.log.error(e.toString());
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
				.exec(ObjectLookUpper.ML_FOLDER + "/c5.0 -f " + ObjectLookUpper.ML_FOLDER
						+ "/input");// >

		ObjectLookUpper.log.info("Reading objects from file");
		// Read result and store in a list
		BufferedReader input = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		String line;
		List<String> inputs = new ArrayList<String>();
		while ((line = input.readLine()) != null) {
			inputs.add(line);
			// log.info(line);
		}
		input.close();

		ObjectLookUpper.log.info("Croppering file");
		this.rules = FileCropper.crop(inputs);

		ObjectLookUpper.log.info("Creating Tree");
		// Create Tree
		try {
			this.treeParser = new TreeParser(this.rules);
		} catch (Exception e) {
			ObjectLookUpper.log.error("Error in creating tree!", e);
		}
	}

	public void populateBloomFilter(List<Pair<String, Integer>> toMoveObj) {
		ObjectLookUpper.log.info("Populating Bloom Filter");
		this.bf = new BloomFilter(ObjectLookUpper.BF_FALSE_POSITIVE_PROB, toMoveObj.size());
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
