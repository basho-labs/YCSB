package com.yahoo.ycsb.generator;

import java.util.concurrent.ConcurrentSkipListMap;

public class ConstThreadCounterGenerator extends IntegerGenerator {

	private ConcurrentSkipListMap<Long, Integer> threadCounters;
	
	public ConstThreadCounterGenerator() {

	}
	
	@Override
	public int nextInt() {	
		return 1;
	}

	@Override
	public double mean() {
		throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
	}

}
