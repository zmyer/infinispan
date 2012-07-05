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
	
	 List<List<TreeElement>> treeList;
	 List<Pair<Integer[], Integer>> values;
	 String rules;
	
	
	public  void main(String args[]) {
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

			createTree(br);
			readValues(br2);

			System.out.println(printArray(values.get(0).left) + " " + values.get(0).right);
			Integer A = null;
			Integer B = null;
			Integer C = null;
			Integer D = null;
			Integer E = 6;
			Integer F = 10;
			Integer G = null;
			Integer H = null;
			Integer[] example = getExample(A, B, C, D, E, F, G, H);

			Integer exampleClass = parseTree(treeList.get(0), example);
			int[][] results = new int[10][10];
			for (Pair<Integer[], Integer> it : values) {
				Integer result = parseTree(treeList.get(0), it.left);
				results[it.right][result]++;
			}
			printMatrix(results);
			System.out.println(exampleClass);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public  void setTreeList(List<List<TreeElement>> tList){
		treeList = tList;
	}
	
	public  List<List<TreeElement>> getTreeList(){
		return treeList;
	}

	private  void printMatrix(int[][] matrix) {
		for(int[] it : matrix){
			for(int it2: it){
				System.out.print(it2 + " ");
			}
			System.out.println();
		}
	}

	private  void readValues(BufferedReader br) {
		try {
			String line = null;
			br.ready();
			values = new ArrayList<Pair<Integer[], Integer>>();
			while ((line = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line, ",");
				Integer A = p(st);
				Integer B = p(st);
				Integer C = p(st);
				Integer D = p(st);
				Integer E = p(st);
				Integer F = p(st);
				Integer G = p(st);
				Integer H = p(st);
				Integer[] example = getExample(A, B, C, D, E, F, G, H);
				Integer val = p(st);
				values.add(new Pair<Integer[], Integer>(example, val));
			}
		//	return values;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Integer p(StringTokenizer st) {
		String current = st.nextToken();
		if (current.equals("N/A")) return null;
		return Integer.parseInt(current);
	}

	private Integer[] getExample(Integer A, Integer B, Integer C, Integer D, Integer E,
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

	public Integer parseTree(List<TreeElement> tree, Integer[] example) {
		Integer returnValue = null;
		for (TreeElement it : tree) {
			if (isMatch(example, it)) {
				assert (returnValue == null);
				if (it.isLeaf) {
					returnValue = it.result;
				} else {
					returnValue = parseTree(it.children, example);
					assert (returnValue != null) : it + " " + printArray(example);
				}
			}
		}
		return returnValue;
	}
	
	public Integer parseTree(Integer[] example) {
		Integer returnValue = null;
		List<TreeElement> tree = treeList.get(0);
		for (TreeElement it : tree) {
			if (isMatch(example, it)) {
				assert (returnValue == null);
				if (it.isLeaf) {
					returnValue = it.result;
				} else {
					returnValue = parseTree(it.children, example);
					assert (returnValue != null) : it + " " + printArray(example);
				}
			}
		}
		return returnValue;
	}

	private  String printArray(Integer[] example) {
		String ret = "[";
		String del = "";
		for (Integer it : example) {
			ret += del + it;
			del = ",";
		}
		ret += "]";
		return ret;
	}

	private  boolean isMatch(Integer[] example, TreeElement elem) {
		int index = getIndex(elem.attribute);
		return checkMatch(elem, example[index]);
	}

	public  void createTree(BufferedReader br) {
		treeList = new ArrayList<List<TreeElement>>();
		try {
			String line = null;

			br.ready();
			while ((line = br.readLine()) != null) {
				int level = parseLevel(line, treeList);
				StringTokenizer tk = new StringTokenizer(line, " :.");
				Character attribute = tk.nextToken().charAt(0);
				Condition condition = parseCondition(tk.nextToken());
				Integer cut = parseCut(tk.nextToken(), condition);
				Integer value = parseValue(tk);
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

	private  void printList(ArrayList<TreeElement> list, int level) {
		for (TreeElement it : list) {
			System.out.print(level + ":" + it + "  ");
			printList(it.children, level + 1);
		}
		System.out.println();
	}

	private  int getIndex(char attribute) {
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

	private  boolean checkMatch(TreeElement elem, Integer value) {
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

	private  Integer parseValue(StringTokenizer tk) {
		if (tk.hasMoreTokens()) return Integer.parseInt(tk.nextToken());
		return null;
	}

	private  Integer parseCut(String cutS, Condition condition) {
		if (condition == Condition.EQ) {
			assert (cutS.equals("N/A"));
			return null;
		} else
			return Integer.valueOf(cutS);
	}

	private  Condition parseCondition(String conditionS) {
		if (conditionS.equals("="))
			return Condition.EQ;
		else if (conditionS.equals("<="))
			return Condition.LE;
		else if (conditionS.equals(">"))
			return Condition.GT;
		else
			throw new RuntimeException(conditionS);
	}

	private  TreeState parseTreeState(int level, int lastLevel) {
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

	private  int parseLevel(String line, List<List<TreeElement>> tree) {
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

