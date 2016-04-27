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

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.workloads.CoreWorkload;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    
    private static final String USE_ALL_TYPE_SCHEMA="alltypeschema";
    private static final String USE_ALL_TYPE_SCHEMA_DEFAULT="false";
    
    private long timestamp;
    private final String host;
    private boolean alltypeschema;
    private HashMap<String, Long> timestampMap;
    
    public RiakTSWorkload() throws UnknownHostException {
        host = InetAddress.getLocalHost().getHostAddress();
    }

    @Override
    public String buildKeyName(long keynum) {
        final String key = super.buildKeyName(keynum + 1); // Add one to the keynum to ensure a timestamp >= 0
        String workerId = String.format("worker-%d", Thread.currentThread().getId());

        final long ts = RiakUtils.getKeyAsLong(key);
        return String.format("%d,%s,%s,%s,%s,%s", ts, key, host, workerId,batchsize,alltypeschema);
    }
    
    @Override
    public void init(Properties p) throws WorkloadException {
        super.init(p);
        this.timestamp = Long.parseLong(p.getProperty(INITIAL_TS_PROPERTY, Long.toString(System.currentTimeMillis())));
        this.alltypeschema = Boolean.parseBoolean(p.getProperty(USE_ALL_TYPE_SCHEMA, USE_ALL_TYPE_SCHEMA_DEFAULT));
        
        this.timestampMap = new HashMap<String, Long>();
    }
}
