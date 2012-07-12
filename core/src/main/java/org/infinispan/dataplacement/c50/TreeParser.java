package org.infinispan.dataplacement.c50;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.infinispan.dataplacement.lookup.ObjectLookUpper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.clearspring.analytics.util.Pair;

public class TreeParser {
	List<List<TreeElement>> treeList;
	List<Pair<Integer[], Integer>> values;
	String rules;
	
	private static final Log log = LogFactory
			.getLog(TreeParser.class);
	
	public TreeParser(List<String> rulesFile) {
		this.treeList = new ArrayList<List<TreeElement>>();

		log.info("Creating tree by rule list");
		
		for (String line : rulesFile) {
			int level = this.parseLevel(line, this.treeList);
			StringTokenizer tk = new StringTokenizer(line, " :.");
			Character attribute = tk.nextToken().charAt(0);
			Condition condition = this.parseCondition(tk.nextToken());
			Integer cut = this.parseCut(tk.nextToken(), condition);
			Integer value = this.parseValue(tk);
			boolean isLeaf = value != null;

			TreeElement parent = null;
			if (level != 0) {
				parent = this.treeList.get(level - 1).get(
						this.treeList.get(level - 1).size() - 1);
			}
			TreeElement newElement = new TreeElement(parent, level);
			this.treeList.get(level).add(newElement);
			if (parent != null) {
				parent.addChild(newElement);
			}
			newElement.attribute = attribute;
			newElement.cut = cut;
			newElement.condition = condition;
			newElement.isLeaf = isLeaf;
			newElement.result = value;
		}
		
		log.info("Tree creation finished.");
		// return tree;
	}
	
	public TreeParser(List<List<TreeElement>> treeList,boolean ignoreMe) {
		this.treeList = treeList;
	}

	public List<List<TreeElement>> getTreeList() {
		return this.treeList;
	}

	public Integer parseTree(List<TreeElement> tree, Integer[] example) {
		Integer returnValue = null;
		for (TreeElement it : tree) {
			if (this.isMatch(example, it)) {
				assert (returnValue == null);
				if (it.isLeaf) {
					returnValue = it.result;
				} else {
					returnValue = this.parseTree(it.children, example);
					assert (returnValue != null) : it + " " + this.printArray(example);
				}
			}
		}
		return returnValue;
	}

	private boolean isMatch(Integer[] example, TreeElement elem) {
		int index = this.getIndex(elem.attribute);
		return this.checkMatch(elem, example[index]);
	}

	private boolean checkMatch(TreeElement elem, Integer value) {
		switch (elem.condition) {
		case EQ:
			assert (elem.cut == null) : elem + " " + value;
			return value == elem.cut;
		case GT:
			if (value == null) return false;
			assert (elem.cut != null) : elem + " " + value;
			return value > elem.cut;
		case LE:
			if (value == null) return false;
			assert (elem.cut != null) : elem + " " + value;
			return value <= elem.cut;
		}
		throw new RuntimeException(); // unreachable, annoying compiler
	}

	public Integer parseTree(Integer[] example) {
		Integer returnValue = null;
		List<TreeElement> tree = this.treeList.get(0);
		for (TreeElement it : tree) {
			if (this.isMatch(example, it)) {
				assert (returnValue == null);
				if (it.isLeaf) {
					returnValue = it.result;
				} else {
					returnValue = this.parseTree(it.children, example);
					assert (returnValue != null) : it + " " + this.printArray(example);
				}
			}
		}
		return returnValue;
	}

	private int getIndex(char attribute) {
		switch (attribute) {
		case 'A':
			return 0;
		case 'B':
			return 1;
		case 'C':
			return 2;
		case 'D':
			return 3;
		case 'E':
			return 4;
		case 'F':
			return 5;
		case 'G':
			return 6;
		case 'H':
			return 7;
		default:
			throw new RuntimeException("undefined attribute");
		}
	}

	private Integer parseValue(StringTokenizer tk) {
		if (tk.hasMoreTokens()) return Integer.parseInt(tk.nextToken());
		return null;
	}

	private Integer parseCut(String cutS, Condition condition) {
		if (condition == Condition.EQ) {
			assert (cutS.equals("N/A"));
			return null;
		} else
			return Integer.valueOf(cutS);
	}

	private Condition parseCondition(String conditionS) {
		if (conditionS.equals("="))
			return Condition.EQ;
		else if (conditionS.equals("<="))
			return Condition.LE;
		else if (conditionS.equals(">"))
			return Condition.GT;
		else
			throw new RuntimeException(conditionS);
	}

	private int parseLevel(String line, List<List<TreeElement>> tree) {
		int counter = 0;
		for (int it = 0; it < line.length(); it++) {
			char c = line.charAt(it);
			if (c == ' ' || c == ':' || c == '.') {
				counter++;
			} else {
				break;
			}
		}
		assert (counter % 4 == 0);
		int level = counter / 4;

		while (tree.size() <= level) {
			tree.add(new ArrayList<TreeElement>());
		}
		return level;
	}

	enum Condition {
		EQ, LE, GT
	}

	private String printArray(Integer[] example) {
		String ret = "[";
		String del = "";
		for (Integer it : example) {
			ret += del + it;
			del = ",";
		}
		ret += "]";
		return ret;
	}
}
