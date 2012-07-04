package org.infinispan.dataplacement.lookup;

public class  InfiniObject implements Comparable<InfiniObject>{
	public String name;
	public Integer number;
	
	public InfiniObject(String name, Integer number){
		this.name = name;
		this.number = number;
	}
	
	public String toString() {
		return name+" "+number;
	}

	@Override
	public int compareTo(InfiniObject arg0) {
		return this.number.compareTo(arg0.number);
	}
	
}