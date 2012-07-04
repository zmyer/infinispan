package org.infinispan.dataplacement.c50;

import java.io.Serializable;
import java.util.ArrayList;


public class TreeElement  implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -706641612338583629L;
	boolean isLeaf;
	Integer result;

	Integer cut;
	char attribute;
	TreeParser.Condition condition;
	ArrayList<TreeElement> children;

	TreeElement parent;
	int level;

	TreeElement(TreeElement parent, int level) {
		this.parent = parent;
		this.level = level;
		this.children = new ArrayList<TreeElement>();
	}

	public void addChild(TreeElement newElement) {
		this.children.add(newElement);
	}

	@Override
	public String toString() {
		String l = this.isLeaf ? " L:" + this.result : "";
		String ret = this.attribute + " " + this.condition + " " + this.cut + l;
		return ret;
	}
}
