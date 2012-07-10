package org.infinispan.dataplacement.c50;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.clearspring.analytics.util.Pair;

public class NameSplitter {


	public static Integer[] splitSingleKey(String key) {
		Integer[] fullKey = { null, null, null, null, null, null, null, null };
		if (key.startsWith("O")) {
			String[] parts = key.split("_");
			String num1, num2, num3;
			fullKey[0] = Integer.parseInt(parts[1]);
			fullKey[1] = Integer.parseInt(parts[2]);
			fullKey[2] = Integer.parseInt(parts[3]);
			return fullKey;
		}
		else if (key.startsWith("W")) {
			String num = key.split("_")[1];
			fullKey[3] = Integer.parseInt(num);
			// splittedKeys.add("N/A,N/A,N/A,"+ num+",N/A,N/A,N/A,N/A");
			return fullKey;
			// keys.set(i,"N/A,N/A,N/A,"+ num+",N/A,N/A,N/A,N/A");
		}
		else if (key.startsWith("D")) {
			String[] parts = key.split("_");
			String num1, num2;
			fullKey[4] = Integer.parseInt(parts[1]); // num1 = parts[1];
			fullKey[5] = Integer.parseInt(parts[2]);// num2 = parts[2];
			// splittedKeys.add("N/A,N/A,N/A,N/A,"+num1 + ","+num2 +",N/A,N/A");
			return fullKey;
			// keys.set(i,"N/A,N/A,N/A,N/A,"+num1 + ","+num2 +",N/A,N/A");
		}
		else if (key.startsWith("S")) {
			String[] parts = key.split("_");
			String num1, num2;
			fullKey[6] = Integer.parseInt(parts[1]);
			fullKey[7] = Integer.parseInt(parts[2]);// num1 = parts[1];
			// num2 = parts[2];
			return fullKey;
			// splittedKeys.add("N/A,N/A,N/A,N/A,N/A,N/A,"+num1 + ","+num2);
			// keys.set(i,"N/A,N/A,N/A,N/A,N/A,N/A,"+num1 + ","+num2);
		}
		return fullKey;
	}

	public static List<Pair<String, Integer>> splitPairKeys(List<Pair<String, Integer>> keys) {
		int size = keys.size();
		List<Pair<String, Integer>> splittedKeys = new ArrayList<Pair<String, Integer>>();
		String key = null;
		for (int i = 0; i < size; ++i) {
			key = keys.get(i).left;
			if (key.startsWith("O")) {
				String[] parts = key.split("_");
				String num1, num2, num3;
				num1 = parts[1];
				num2 = parts[2];
				num3 = parts[3];
				splittedKeys.add(new Pair<String, Integer>(num1 + "," + num2 + "," + num3
						+ ",N/A,N/A,N/A,N/A,N/A", keys.get(i).right));
				// keys.set(i,num1 + ","+num2 + ","+num3
				// +",N/A,N/A,N/A,N/A,N/A");
			}
			else if (key.startsWith("W")) {
				String num = key.split("_")[1];
				splittedKeys.add(new Pair<String, Integer>("N/A,N/A,N/A," + num
						+ ",N/A,N/A,N/A,N/A", keys.get(i).right));
				// keys.set(i,"N/A,N/A,N/A,"+ num+",N/A,N/A,N/A,N/A");
			}
			else if (key.startsWith("D")) {
				String[] parts = key.split("_");
				String num1, num2;
				num1 = parts[1];
				num2 = parts[2];
				splittedKeys.add(new Pair<String, Integer>("N/A,N/A,N/A,N/A," + num1 + "," + num2
						+ ",N/A,N/A", keys.get(i).right));
				// keys.set(i,"N/A,N/A,N/A,N/A,"+num1 + ","+num2 +",N/A,N/A");
			}
			else if (key.startsWith("S")) {
				String[] parts = key.split("_");
				String num1, num2;
				num1 = parts[1];
				num2 = parts[2];
				splittedKeys.add(new Pair<String, Integer>("N/A,N/A,N/A,N/A,N/A,N/A," + num1 + ","
						+ num2, keys.get(i).right));
				// keys.set(i,"N/A,N/A,N/A,N/A,N/A,N/A,"+num1 + ","+num2);
			}
		}
		return splittedKeys;
	}

	public static void writeKeys(BufferedWriter bw, List<String> keys, int counter)
			throws IOException {

		for (String key : keys) {
			bw.write(key + " 2" + counter + "\n");
		}
	}

	public static void writePairKeys(BufferedWriter bw, List<Pair<String, Integer>> keys)
			throws IOException {
		for (Pair<String, Integer> key : keys) {
			bw.write(key.left + "," + key.right + "\n");
		}
	}

}
