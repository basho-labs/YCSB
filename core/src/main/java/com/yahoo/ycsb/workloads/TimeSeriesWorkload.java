package com.yahoo.ycsb.workloads;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.ThreadCounterGenerator;

public class TimeSeriesWorkload extends CoreWorkload {
	
	private static String hostname;
	
	@Override
	public void init(Properties p) throws WorkloadException
	{
		try {
			this.hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			this.hostname = "localhost";
		}
		super.init(p);
		
		super.keysequence = new ThreadCounterGenerator();
	}
	
	@Override
	public String buildKeyName(long keynum) {
		if (!orderedinserts)
 		{
 			keynum=Utils.hash(keynum);
 		}
		return keynum + "," + this.hostname + ",worker-" + Thread.currentThread().getId();
	}
}
