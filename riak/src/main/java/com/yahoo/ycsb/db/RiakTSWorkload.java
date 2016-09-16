/*
 * Copyright 2015 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.workloads.CoreWorkload;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Properties;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 */
public class RiakTSWorkload extends CoreWorkload {

    /**
     * Initial value of time field
     */
    public static final String INITIAL_TS_PROPERTY="initialtimestamp";

    /**
     * Use generated timestamp instead of using part of Key as a timestamp.
     *
     * If 'false' then INITIAL_TS_PROPERTY will be ignored
     */
    public static final String USE_OWN_TIMESTAMP="useowntimestamp";
    
    private long timestamp;
    private boolean useOwnTimestamp;
    private final String host;
    private HashMap<String, Long> timestampMap;
    
    public RiakTSWorkload() throws UnknownHostException {
        host = InetAddress.getLocalHost().getHostAddress();
    }

    @Override
    public String buildKeyName(long keynum) {
        final String key = super.buildKeyName(keynum);
        String workerId = String.format("worker-%d", Thread.currentThread().getId());

        final long ts;
        if (useOwnTimestamp) {
        	if (timestampMap.containsKey(workerId))
        	{
        		ts = (Long) timestampMap.get(workerId) + 1;
        		timestampMap.put(workerId, ts);
        	} else
        	{
        		ts = this.timestamp;
        		this.timestampMap.put(workerId, ts);
        	}
        } else {
            ts = RiakUtils.getKeyAsLong(key);
        }

        return String.format("%d,%s,%s,%s", ts, key, host, workerId);
    }

    @Override
    public void init(Properties p) throws WorkloadException {
        super.init(p);
        this.timestamp = Long.parseLong(p.getProperty(INITIAL_TS_PROPERTY, Long.toString(System.currentTimeMillis())));
        this.useOwnTimestamp = Boolean.parseBoolean(p.getProperty(USE_OWN_TIMESTAMP, "false"));
        this.timestampMap = new HashMap<String, Long>();
    }
}
