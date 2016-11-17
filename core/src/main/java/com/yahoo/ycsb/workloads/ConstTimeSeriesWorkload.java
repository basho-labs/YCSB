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
import com.yahoo.ycsb.generator.ConstThreadCounterGenerator;

public class ConstTimeSeriesWorkload extends TimeSeriesWorkload {
	
	private static String hostname;
	
	@Override
	public void init(Properties p) throws WorkloadException
	{
		super.init(p);
		
		super.keysequence = new ConstThreadCounterGenerator();
	}
}
