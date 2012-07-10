package org.infinispan.dataplacement.lookup;

import java.io.Serializable;
import java.util.BitSet;

public class BloomFilter implements Serializable {
	private static final long serialVersionUID = 8896890490944539799L;
	private int numHash, filterSize;
	private double bitsPerElement;
	private BitSet filter;
	private double falsePositiveRate;

	public BloomFilter(int numElements, double falsePositiveRate) {
		this.falsePositiveRate = falsePositiveRate;
		this.bitsPerElement = Math.ceil(Math.log(1 / falsePositiveRate) / Math.pow(Math.log(2), 2));
		this.numHash = (int) Math.ceil(Math.log(2) * this.bitsPerElement);

		this.filter = new BitSet((int) Math.ceil(this.bitsPerElement * falsePositiveRate));
		this.filterSize = this.filter.size();
	}

	public int getnumHash() {
		return this.numHash;
	}

	public double falsePositiveRate() {
		return this.falsePositiveRate;
	}

	public double getBitsPerElement() {
		return this.bitsPerElement;
	}

	public double getFilterSize() {
		return this.filterSize;
	}

	public boolean contains(byte[] elementId) {
		int hash1 = this.murmurHash(elementId, elementId.length, 0);
		int hash2 = this.murmurHash(elementId, elementId.length, hash1);
		for (int i = 0; i < this.numHash; i++) {
			if (!this.filter.get(Math.abs((hash1 + i * hash2) % this.filterSize)))
				return false;
		}
		return true;
	}

	public boolean contains(String str) {
		return this.contains(str.getBytes());
	}

	public boolean add(byte[] elementId) {
		int hash1 = this.murmurHash(elementId, elementId.length, 0);
		int hash2 = this.murmurHash(elementId, elementId.length, hash1);
		for (int i = 0; i < this.numHash; i++) {
			this.filter.set(Math.abs((hash1 + i * hash2) % this.filterSize), true);
		}
		return true;
	}

	public void add(String str) {
		this.add(str.getBytes());
	}

	private int murmurHash(byte[] data, int length, int seed) {
		int m = 0x5bd1e995;
		int r = 24;

		int h = seed ^ length;

		int len_4 = length >> 2;

		for (int i = 0; i < len_4; i++) {
			int i_4 = i << 2;
			int k = data[i_4 + 3];
			k = k << 8;
			k = k | (data[i_4 + 2] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 1] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 0] & 0xff);
			k *= m;
			k ^= k >>> r;
			k *= m;
			h *= m;
			h ^= k;
		}

		// avoid calculating modulo
		int len_m = len_4 << 2;
		int left = length - len_m;

		if (left != 0) {
			if (left >= 3) {
				h ^= data[length - 3] << 16;
			}
			if (left >= 2) {
				h ^= data[length - 2] << 8;
			}
			if (left >= 1) {
				h ^= data[length - 1];
			}

			h *= m;
		}

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;

		return h;
	}
}