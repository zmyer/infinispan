package org.infinispan.dataplacement.c50;

import java.util.ArrayList;
import java.util.List;

public class FileCropper {
	public static List<String> crop(List<String> inputs) {
		List<String> toReturn = new ArrayList<String>();
		boolean beforeTree = true, afterTree = false;
		String line;
		for (int i = 0; i < inputs.size(); ++i) {
			line = inputs.get(i);
			if (true == beforeTree) {
				if (line.startsWith("Decision tree:") == true) {
					beforeTree = false;
					continue;
				}
			}
			else if (false == afterTree) {
				if (line.startsWith("Evaluation")) {
					afterTree = true;
					continue;
				}
			}

			if (false == beforeTree && false == afterTree && !line.isEmpty()) {
				toReturn.add(line);
			}
		}
		return toReturn;
	}
}
