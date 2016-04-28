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

import com.basho.riak.client.api.commands.timeseries.Delete;
import com.basho.riak.client.api.commands.timeseries.Fetch;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;

import java.util.*;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 */
public class RiakTSClient extends AbstractRiakClient {
    
	@Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {

        final Row row = RiakUtils.asTSRow(key, Collections.EMPTY_MAP);

        dumpOperation(row, "READ:TRY");

        QueryResult response = QueryResult.EMPTY;
        
        // Attempt the read up to the configured
        // number of re-try counts. Once reached, the
        // operation will fail
        for (int i=0; i<config().readRetryCount()+1; ++i) {
            final Fetch cmd = new Fetch.Builder(table, row)
                    .build();

            try {
                if (config().isDebug() && i!=0) {
                    dumpOperation(row, "READ:RE-TRY(%d)", i);
                }
                response = riakClient.execute(cmd);
                if ( !QueryResult.EMPTY.equals(response)){
                    break;
                }
            } catch (Exception e) {
                dumpOperationException(e, row, "READ:FAILED");
                return Status.ERROR;
            }
        }

        // Check if the result is empty
        if ( QueryResult.EMPTY.equals(response))
        {
            dumpOperation(row, "READ:RESULT - NOT FOUND");
            return Status.NOT_FOUND;
        }

        dumpOperation(response.iterator().next(), "READ:RESULT - OK, the first of %d", response.getRowsCount());

        // Insure that just a single row was returned
        assert response.getRowsCount() == 1;
        final Vector<HashMap<String, ByteIterator>> v = RiakUtils.asSYCSBResults(response);
        assert v.size() == 1;

        result.putAll( v.get(0));
        return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        final Map.Entry<List<ColumnDescription>,Row> data = RiakUtils.asTSRowWithColumns(startkey, Collections.EMPTY_MAP);

        int scanSize = recordcount;
        if (config().scanSize() > 0) {
            scanSize = config().scanSize();
        }

        dumpOperation(data.getValue(), "SCAN:TRY (%d)", scanSize);

        final Iterator<Cell> iterator = data.getValue().iterator();

        final String host = iterator.next().getVarcharAsUTF8String();
        final String worker = iterator.next().getVarcharAsUTF8String();
        final long startTime = iterator.next().getTimestamp();

        // Construct the query SQL 
        final String query = String.format("SELECT * FROM %s " +
                " WHERE " +
                    " host = '%s' " +
                    " AND worker = '%s' " +
                    " AND time >= %d AND time < %d",
                    table, host, worker, startTime, startTime + scanSize
                );

        final Query cmd = new Query.Builder(query).build();

        final QueryResult response;
        try {
            response = riakClient.execute(cmd);
        } catch (Exception e) {
            dumpOperationException(e, data.getValue(), "SCAN:FAILED");
            return Status.ERROR;
        }

        if ( QueryResult.EMPTY.equals(response))
        {
            dumpOperation(data.getValue(), "SCAN:RESULT - NOT FOUND");
            return Status.NOT_FOUND;
        }

        dumpOperation(response.iterator().next(), "SCAN:RESULT - OK, the 1st of %d", response.getRowsCount());

        final Vector<HashMap<String, ByteIterator>> v = RiakUtils.asSYCSBResults(response);
        result.addAll( v );
        return Status.OK;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        return insert(table, key, values);
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        final List<Row> rows = RiakUtils.asBatchedTSRow(key, values);
        dumpOperation(rows, "INSERT:TRY");

        final Store cmd = new Store.Builder(table)
                .withRows(rows)
                .build();

        try {
            riakClient.execute(cmd);
        } catch (Exception e) {
        	System.out.println(e.getMessage());
            return Status.ERROR;
        }

        return Status.OK;
    }

	@Override
    public Status delete(String table, String key) {
        final Map.Entry<List<ColumnDescription>,Row> data = RiakUtils.asTSRowWithColumns(key, Collections.EMPTY_MAP);

        dumpOperation(data.getValue(), "DELETE:TRY");
        final Delete cmd = new Delete.Builder(table, data.getValue())
                .build();

        try {
            riakClient.execute(cmd);
            dumpOperation(data.getValue(), "DELETE:RESULT - OK");
        } catch (Exception e) {
            dumpOperationException(e, data.getValue(), "DELETE:FAILED");
            return Status.ERROR;
        }

        return Status.OK;
    }
}
