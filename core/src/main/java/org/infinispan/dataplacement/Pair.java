package org.infinispan.dataplacement;

public class Pair<X,Y>{
    public X left;
    public Y right;
    
    public Pair(X left, Y right){
            this.left=left;
            this.right=right;
    }
    
    public String toString()
    { return "(" + left + "," + right + ")"; }
}

