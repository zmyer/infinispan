package org.infinispan.dataplacement.c50;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.clearspring.analytics.util.Pair;


public class TreeParser {
	
	static List<List<TreeElement>> treeList;
	static List<Pair<Integer[], Integer>> values;
	static String rules;
	
	
	public static void main(String args[]) {
		NumberFormat formatter = new DecimalFormat("#0.0000");
		try {
			BufferedReader br = null;
			BufferedReader br2 = null;
			if (args.length == 2) {
				br = new BufferedReader(new FileReader(args[0]));
				br2 = new BufferedReader(new FileReader(args[1]));
			} else {
				System.err.println("TreeParser.java: wrong number of arguments. Expected 2");
				System.exit(-1);
			}

			TreeParser.createTree(br);
			TreeParser.readValues(br2);

			System.out.println(TreeParser.printArray(values.get(0).left) + " " + values.get(0).right);
			Integer A = null;
			Integer B = null;
			Integer C = null;
			Integer D = null;
			Integer E = 6;
			Integer F = 10;
			Integer G = null;
			Integer H = null;
			Integer[] example = TreeParser.getExample(A, B, C, D, E, F, G, H);

			Integer exampleClass = TreeParser.parseTree(treeList.get(0), example);
			int[][] results = new int[10][10];
			for (Pair<Integer[], Integer> it : values) {
				Integer result = TreeParser.parseTree(treeList.get(0), it.left);
				results[it.right][result]++;
			}
			TreeParser.printMatrix(results);
			System.out.println(exampleClass);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void setTreeList(List<List<TreeElement>> tList){
		treeList = tList;
	}
	
	public static List<List<TreeElement>> getTreeList(){
		return treeList;
	}

	private static void printMatrix(int[][] matrix) {
		for(int[] it : matrix){
			for(int it2: it){
				System.out.print(it2 + " ");
			}
			System.out.println();
		}
	}

	private static void readValues(BufferedReader br) {
		try {
			String line = null;
			br.ready();
			values = new ArrayList<Pair<Integer[], Integer>>();
			while ((line = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line, ",");
				Integer A = TreeParser.p(st);
				Integer B = TreeParser.p(st);
				Integer C = TreeParser.p(st);
				Integer D = TreeParser.p(st);
				Integer E = TreeParser.p(st);
				Integer F = TreeParser.p(st);
				Integer G = TreeParser.p(st);
				Integer H = TreeParser.p(st);
				Integer[] example = TreeParser.getExample(A, B, C, D, E, F, G, H);
				Integer val = TreeParser.p(st);
				values.add(new Pair<Integer[], Integer>(example, val));
			}
		//	return values;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static Integer p(StringTokenizer st) {
		String current = st.nextToken();
		if (current.equals("N/A")) return null;
		return Integer.parseInt(current);
	}

	private static Integer[] getExample(Integer A, Integer B, Integer C, Integer D, Integer E,
			Integer F, Integer G, Integer H) {
		Integer[] ret = new Integer[8];
		ret[0] = A;
		ret[1] = B;
		ret[2] = C;
		ret[3] = D;
		ret[4] = E;
		ret[5] = F;
		ret[6] = G;
		ret[7] = H;
		return ret;
	}

	public static Integer parseTree(List<TreeElement> tree, Integer[] example) {
		Integer returnValue = null;
		for (TreeElement it : tree) {
			if (TreeParser.isMatch(example, it)) {
				assert (returnValue == null);
				if (it.isLeaf) {
					returnValue = it.result;
				} else {
					returnValue = TreeParser.parseTree(it.children, example);
					assert (returnValue != null) : it + " " + TreeParser.printArray(example);
				}
			}
		}
		return returnValue;
	}
	
	public static Integer parseTree(Integer[] example) {
		Integer returnValue = null;
		List<TreeElement> tree = treeList.get(0);
		for (TreeElement it : tree) {
			if (TreeParser.isMatch(example, it)) {
				assert (returnValue == null);
				if (it.isLeaf) {
					returnValue = it.result;
				} else {
					returnValue = TreeParser.parseTree(it.children, example);
					assert (returnValue != null) : it + " " + TreeParser.printArray(example);
				}
			}
		}
		return returnValue;
	}

	private static String printArray(Integer[] example) {
		String ret = "[";
		String del = "";
		for (Integer it : example) {
			ret += del + it;
			del = ",";
		}
		ret += "]";
		return ret;
	}

	private static boolean isMatch(Integer[] example, TreeElement elem) {
		int index = TreeParser.getIndex(elem.attribute);
		return TreeParser.checkMatch(elem, example[index]);
	}

	public static void createTree(BufferedReader br) {
		treeList = new ArrayList<List<TreeElement>>();
		try {
			String line = null;

			br.ready();
			while ((line = br.readLine()) != null) {
				int level = TreeParser.parseLevel(line, treeList);
				StringTokenizer tk = new StringTokenizer(line, " :.");
				Character attribute = tk.nextToken().charAt(0);
				Condition condition = TreeParser.parseCondition(tk.nextToken());
				Integer cut = TreeParser.parseCut(tk.nextToken(), condition);
				Integer value = TreeParser.parseValue(tk);
				boolean isLeaf = value != null;

				TreeElement parent = null;
				if (level != 0) {
					parent = treeList.get(level - 1).get(treeList.get(level - 1).size() - 1);
				}
				TreeElement newElement = new TreeElement(parent, level);
				treeList.get(level).add(newElement);
				if (parent != null) {
					parent.addChild(newElement);
				}
				newElement.attribute = attribute;
				newElement.cut = cut;
				newElement.condition = condition;
				newElement.isLeaf = isLeaf;
				newElement.result = value;
			}
			br.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		//return tree;
	}

	private static void printList(ArrayList<TreeElement> list, int level) {
		for (TreeElement it : list) {
			System.out.print(level + ":" + it + "  ");
			TreeParser.printList(it.children, level + 1);
		}
		System.out.println();
	}

	private static int getIndex(char attribute) {
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

	private static boolean checkMatch(TreeElement elem, Integer value) {
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

	private static Integer parseValue(StringTokenizer tk) {
		if (tk.hasMoreTokens()) return Integer.parseInt(tk.nextToken());
		return null;
	}

	private static Integer parseCut(String cutS, Condition condition) {
		if (condition == Condition.EQ) {
			assert (cutS.equals("N/A"));
			return null;
		} else
			return Integer.valueOf(cutS);
	}

	private static Condition parseCondition(String conditionS) {
		if (conditionS.equals("="))
			return Condition.EQ;
		else if (conditionS.equals("<="))
			return Condition.LE;
		else if (conditionS.equals(">"))
			return Condition.GT;
		else
			throw new RuntimeException(conditionS);
	}

	private static TreeState parseTreeState(int level, int lastLevel) {
		TreeState treeState;
		if (lastLevel < level) {
			treeState = TreeState.NEW;
		} else if (lastLevel == level) {
			treeState = TreeState.SAME;
		} else {
			treeState = TreeState.OLD;
		}
		return treeState;
	}

	private static int parseLevel(String line, List<List<TreeElement>> tree) {
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

	enum TreeState {
		SAME, NEW, OLD
	}
}

