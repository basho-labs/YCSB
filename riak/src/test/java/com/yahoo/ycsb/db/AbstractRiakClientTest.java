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

import com.basho.riak.client.core.RiakCluster;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.measurements.Measurements;
import org.junit.After;
import org.junit.Before;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 */
public abstract class AbstractRiakClientTest<C extends AbstractRiakClient> {

    protected static class WorkloadData {
        public String table;
        public String key;
        public HashMap<String, ByteIterator> values;
    }

    private final Class<C> clientClass;
    private final Class<? extends Workload> workloadClass;
    private C cli;
    protected String bucket = "ycsb-test";
    protected RiakFunctions riakFunctions;

    private Workload workload;
    protected WorkloadData dataSample;

    protected AbstractRiakClientTest(Class<C> clientClass, Class<? extends Workload>workloadClass) {
        this.clientClass = clientClass;
        this.workloadClass = workloadClass;
    }

    @Before
    public void setUp() throws Exception {
        // -- Spawn cli
        cli = clientClass.newInstance();

        // Load default properties
        final InputStream ios = getClass().getResourceAsStream("/riak-test.properties");
        assert ios != null;
        cli.getProperties().load(ios);
        cli.init();


        final RiakCluster cluster = cli.config().createRiakCluster();
        cluster.start();

        riakFunctions = RiakFunctions.create(cluster);

        // -- Initialize workload
        Measurements.setProperties(new Properties());
        workload = workloadClass.newInstance();

        final InputStream ios2 = getClass().getResourceAsStream("/workload-test.properties");
        final Properties props = new Properties();
        props.load(ios2);

        workload.init(props);

        dataSample = generateSampleData();
    }

    @After
    public void tearDown() throws Exception {
        cli().cleanup();
        riakFunctions.close();
    }

    protected C cli() {
        return cli;
    }

    protected WorkloadData generateSampleData() {
        final WorkloadData sample = new WorkloadData();
        final DB fakeDB = new DB(){
            @Override
            public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Status update(String table, String key, HashMap<String, ByteIterator> values) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
                sample.table = table;
                sample.key = key;
                sample.values = values;
                return Status.OK;
            }

            @Override
            public Status delete(String table, String key) {
                throw new UnsupportedOperationException();
            }
        };

        workload.doInsert(fakeDB, null);
        return sample;
    }

}
