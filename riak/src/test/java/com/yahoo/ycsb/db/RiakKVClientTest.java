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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import com.yahoo.ycsb.workloads.CoreWorkload;
import org.junit.*;
import org.junit.runners.MethodSorters;

/**
 * @author Brian McClain <bmcclain at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 */
@FixMethodOrder(MethodSorters.JVM)
public class RiakKVClientTest extends AbstractRiakClientTest<RiakKVClient>{
    private String key = "42";

    public RiakKVClientTest() {
        super(RiakKVClient.class, CoreWorkload.class);
    }

    /**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
        super.setUp();
        riakFunctions.resetAndEmptyBucket(cli().config().mkNamespaceFor(bucket));
	}

    /**
     * Test method for RiakDBClient.insert(java.lang.String, java.lang.String, java.util.HashMap)
     */
    @Test
    public void testInsert() throws ExecutionException, InterruptedException {
        HashMap<String, String> values = new HashMap<String, String>();
        values.put("first_name", "Dave");
        values.put("last_name", "Parfitt");
        values.put("city", "Buffalo, NY");
        assertEquals(Status.OK, cli().insert(bucket, key, StringByteIterator.getByteIteratorMap(values)));

        riakFunctions.awaitWhileAvailable( cli().config().mkLocationFor(bucket, key));
    }

	/**
	 * Test method for RiakDBClient.read(java.lang.String, java.lang.String, java.util.Set, java.util.HashMap)
	 */
	@Test
	public void testRead() {
		Set<String> fields = new HashSet<String>();
        fields.add("first_name");
        fields.add("last_name");
        fields.add("city");
        HashMap<String, ByteIterator> results = new HashMap<String, ByteIterator>();
        assertEquals(Status.OK, cli().read(bucket, key, fields, results));
	}

	/**
	 * Test method for RiakDBClient.scan(java.lang.String, java.lang.String, int, java.util.Set, java.util.Vector)
	 */
	@Test
	public void testScan() {
		Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();
		assertEquals(Status.OK, cli().scan(bucket, "user5947069136552588163", 7, null, results));
	}

	/**
	 * Test method for RiakDBClient.update(java.lang.String, java.lang.String, java.util.HashMap)
	 */
	@Test
	public void testUpdate() {
		HashMap<String, String> values = new HashMap<String, String>();
        values.put("first_name", "Dave");
        values.put("last_name", "Parfitt");
        values.put("city", "Buffalo, NY");
		assertEquals(Status.OK, cli().update(bucket, key, StringByteIterator.getByteIteratorMap(values)));
	}

	/**
	 * Test method for RiakDBClient.delete(java.lang.String, java.lang.String)
	 */
	@Test
	public void testDelete() {
        assertEquals(Status.OK, cli().delete(bucket, key));
	}

}
