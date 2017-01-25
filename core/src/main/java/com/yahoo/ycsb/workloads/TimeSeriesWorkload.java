package com.yahoo.ycsb.workloads;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.ThreadCounterGenerator;

public class TimeSeriesWorkload extends CoreWorkload {
	
	private static String hostname;
	
	public static final String THREAD_COUNT_PROPERTY="threadcount";
	private static int threadCount;
	
	public static final String LOAD_RECORD_COUNT_PROPERTY="loadrecordcount";
	private static int loadRecordCount;
	
	public static final String LOAD_THREAD_COUNT_PROPERTY="loadthreadcount";
	private static int loadThreadCount;
	
	private int delta;
	
	@Override
	public void init(Properties p) throws WorkloadException
	{
	  super.init(p);
	  
		try {
			this.hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			this.hostname = "localhost";
		}
		
		threadCount = Integer.parseInt(p.getProperty(THREAD_COUNT_PROPERTY, "1"));
		loadRecordCount = Integer.parseInt(p.getProperty(LOAD_RECORD_COUNT_PROPERTY, Integer.toString(super.recordcount)));
		loadThreadCount = Integer.parseInt(p.getProperty(LOAD_THREAD_COUNT_PROPERTY, Integer.toString(threadCount)));
		delta = super.delta;
	
		super.keysequence = new ThreadCounterGenerator(delta);
	}
	
	@Override
	public String buildKeyName(long keynum) {
		if (!orderedinserts)
 		{
 			keynum=Utils.hash(keynum);
 		}
		return normalizeKeyNum(keynum) + "," + this.hostname + ",worker-" + normalizeThreadId(Thread.currentThread().getId());
	}
	
	private long normalizeKeyNum(long keynum) {
	  if (keynum > loadRecordCount) {
	    return ThreadLocalRandom.current().nextLong(loadRecordCount);
	  } else {
	    return keynum;
	  }
	}
	
	private long normalizeThreadId(long workerId)	{
	  if (workerId > loadThreadCount) {
	    return ThreadLocalRandom.current().nextLong(loadThreadCount);
	  } else {
	    return workerId;
	  }
	}
	
}
