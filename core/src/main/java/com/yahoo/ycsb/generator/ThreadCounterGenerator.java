package com.yahoo.ycsb.generator;

import java.util.concurrent.ConcurrentSkipListMap;

public class ThreadCounterGenerator extends IntegerGenerator {

	private ConcurrentSkipListMap<Long, Integer> threadCounters;
	private int delta;
	
	public ThreadCounterGenerator() {
		this.threadCounters = new ConcurrentSkipListMap<Long, Integer>();
		this.delta = 1;
	}
	
	public ThreadCounterGenerator(int delta) {
	  this.threadCounters = new ConcurrentSkipListMap<Long, Integer>();
	  this.delta = delta;
	}
	
	@Override
	public int nextInt() {
		long threadID = Thread.currentThread().getId();
		int key = 1;
		if (this.threadCounters.containsKey(threadID)) {
			key = this.threadCounters.get(threadID);
		}
		
		this.threadCounters.put(threadID, key+this.delta);		
		return key;
	}

	@Override
	public double mean() {
		throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
	}

}
