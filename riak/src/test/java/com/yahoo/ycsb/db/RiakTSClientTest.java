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

import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.yahoo.ycsb.*;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 */
@FixMethodOrder(MethodSorters.JVM)
public class RiakTSClientTest extends AbstractRiakClientTest<RiakTSClient> {
    private static final Boolean useTTB = false;
    public RiakTSClientTest() {
        super(RiakTSClient.class, RiakTSWorkload.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        if (!useTTB) {
            // TTB'ed GET bucket props is not supported in the moment
            checkTsBucketExistence();
        }
    }

    private void checkTsBucketExistence() throws ExecutionException, InterruptedException {
        final String table = dataSample.table;
        if (!riakFunctions.isTSBucketExists(new Namespace(table, table))){
            final List<ColumnDescription> columns = RiakUtils.asTSRowWithColumns(dataSample.key, dataSample.values).getKey();


            // TODO consider to implement more universal command generator
            final StringBuilder sb = new StringBuilder();
            for (ColumnDescription c: columns){
                if (sb.length() > 0){
                    sb.append(", ");
                }

                sb.append(c.getName())
                        .append(' ')
                        .append(c.getType().name())
                        .append(' ')
                        .append("NOT NULL");
            }

            final String bucketCreationCmd = String.format("./riak-admin bucket-type create %s '{\"props\":{\"n_val\":3, \"table_def\": " +
                            "\"CREATE TABLE %s (%s, primary key (%s))\"}}'",
                    table,
                    table,
                    sb.toString(),
                    //"(okey, worker, quantum(time, 10, s)), okey, worker, time"
                    "(host, worker, quantum(time, 10, s)), host, worker, time"
            );

            Assert.fail("Bucket type for Time Series test data is not created.\n" +
                    "To create and activate TS test bucket, please use the following commands:\n" +
                    "\t" + bucketCreationCmd + "\n" +
                    "\t" + "./riak-admin bucket-type activate " + table);
        }
    }

    @Test(timeout = 5000)
    public void testInsert() throws Exception {
        final QueryResult r = storeAndWaitAvailability(dataSample);
        assert r != null;
    }

    @Test(timeout = 5000)
    public void testDelete() throws Exception {
        storeAndWaitAvailability(dataSample);
        final Status result = cli().delete(dataSample.table, dataSample.key);
        assertEquals(Status.OK, result);

        final Row keys = RiakUtils.asTSRow(dataSample.key, Collections.EMPTY_MAP);
        final QueryResult r = riakFunctions.awaitWhileAvailable(dataSample.table, keys, 3);
        assertEquals(QueryResult.EMPTY, r);
    }

    @Test(timeout = 5000)
    public void testRead() throws Exception {
        storeAndWaitAvailability(dataSample);
        final HashMap<String, ByteIterator> results = new HashMap<String, ByteIterator>();

        final Status result = cli().read(dataSample.table, dataSample.key, dataSample.values.keySet(), results);
        assertEquals(Status.OK, result);
    }

    @Test(timeout = 5000)
    public void testUpdate() throws Exception {
        storeAndWaitAvailability(dataSample);
        final String newValue = UUID.randomUUID().toString();
        final String changedField = dataSample.values.keySet().iterator().next();
        dataSample.values.put(changedField, new StringByteIterator(newValue));

        final Status result = cli().update(dataSample.table, dataSample.key, dataSample.values);
        assertEquals(Status.OK, result);

        final Row keys = RiakUtils.asTSRow(dataSample.key, Collections.EMPTY_MAP);
        for (;;){
            final QueryResult r = riakFunctions.awaitWhileAvailable(dataSample.table, keys, 3);
            assertEquals(1, r.getRowsCount());

            final Cell c = cellByColumnName(changedField, r.getColumnDescriptionsCopy(), r.iterator().next());
            if (newValue.equals(c.getVarcharAsUTF8String())){
                break;
            }
        }
    }

    @Test(timeout = 10000)
    public void testScan() throws Exception {
        final int qnt = 100;
        final int offset = 25;
        final int limit = 30;
        final List<WorkloadData> data = new ArrayList<WorkloadData>(qnt);

        while (data.size() < qnt){
            final WorkloadData d = generateSampleData();
            data.add(d);
            storeAndWaitAvailability(d);
        }


        final List<WorkloadData> chunk = data.subList(offset, offset + limit);
        final WorkloadData theFirst = chunk.get(0);
        final Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();

        assertEquals(Status.OK, cli().scan(theFirst.table, theFirst.key, offset, dataSample.values.keySet(), results));
        assertEquals(offset, results.size());
    }

    @Test
    public void returnNOTFOUND_if_readNonexistent() throws Exception {
        final Row keys = RiakUtils.asTSRow(dataSample.key, Collections.EMPTY_MAP);

        // to be 100% sure that value doesn't exist
        assertEquals(QueryResult.EMPTY, riakFunctions.awaitWhileAvailable(dataSample.table, keys, 3));

        final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        assertEquals(Status.NOT_FOUND, cli().read(dataSample.table, dataSample.key, dataSample.values.keySet(), result));
    }

    @Test
    public void returnOK_if_deleteNonexistent() throws Exception {
        final Row keys = RiakUtils.asTSRow(dataSample.key, Collections.EMPTY_MAP);

        // to be 100% sure that value doesn't exist
        assertEquals(QueryResult.EMPTY, riakFunctions.awaitWhileAvailable(dataSample.table, keys, 3));

        assertEquals(Status.OK, cli().delete(dataSample.table, dataSample.key));
    }

    private QueryResult storeAndWaitAvailability(WorkloadData data) throws Exception {
        final Status status = cli().insert(data.table, data.key, data.values);
        assertEquals(Status.OK, status);

        final Row keys = RiakUtils.asTSRow(data.key, Collections.EMPTY_MAP);
        return riakFunctions.awaitWhileAvailable(data.table, keys);
    }

    private static Cell cellByColumnName(String column, List<ColumnDescription> columns,  Row r) {
        int idx = -1;
        for (int i=0; i<columns.size(); ++i){
            if (columns.get(i).getName().equals(column)){
                idx = i;
                break;
            }
        }
        if (idx < 0){
            throw new IllegalStateException(String.format("Changed field '%s' wasn't been returned", column));
        }

        return RiakUtils.advance(r.iterator(),idx).next();
    }
}
