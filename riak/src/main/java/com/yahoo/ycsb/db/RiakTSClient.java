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
    public RiakTSClient() {
        System.out.print("\n\n\n RIAK TS Client initialized\n\n\n");
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        final Row row = RiakUtils.asTSRow(key, Collections.EMPTY_MAP);

        dumpOperation(row, "READ:TRY");

        QueryResult response = QueryResult.EMPTY;
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
                logger.error("READ FAILED: UE", e);
                return Status.ERROR;
            }
        }


        if ( QueryResult.EMPTY.equals(response))
        {
            dumpOperation(null, "READ:RESULT - NOT FOUND");
            return Status.NOT_FOUND;
        }

        dumpOperation(response.iterator().next(), "READ:RESULT - OK, teh 1st of %d", response.getRowsCount());

        assert response.getRowsCount() == 1;
        final Vector<HashMap<String, ByteIterator>> v = RiakUtils.asSYCSBResults(response);
        assert v.size() == 1;

        result.putAll( v.get(0));
        return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        final Map.Entry<List<ColumnDescription>,Row> data = RiakUtils.asTSRowWithColumns(startkey, Collections.EMPTY_MAP);

        dumpOperation(data.getValue(), "SCAN:TRY (%d)", recordcount);

        final Iterator<Cell> iterator = data.getValue().iterator();

        final String host = iterator.next().getVarcharAsUTF8String();
        final String worker = iterator.next().getVarcharAsUTF8String();
        final long startTime = iterator.next().getTimestamp();

        final String query = String.format("SELECT * FROM %s " +
                " WHERE " +
                    " host = '%s' " +
                    " AND worker = '%s' " +
                    " AND time >= %d AND time < %d",
                    table, host, worker, startTime, startTime + recordcount
                );

        final Query cmd = new Query.Builder(query).build();

        final QueryResult response;
        try {
            response = riakClient.execute(cmd);
        } catch (Exception e) {
            logger.error("SCAN FAILED: UE", e);
            return Status.ERROR;
        }

        if ( QueryResult.EMPTY.equals(response))
        {
            dumpOperation(null, "SCAN:RESULT - NOT FOUND");
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
        final Map.Entry<List<ColumnDescription>,Row> data = RiakUtils.asTSRowWithColumns(key, values);
        dumpOperation(data.getValue(), "UPSERT:TRY");

        final Store cmd = new Store.Builder(table)
                .withRows(Collections.singleton(data.getValue()))
                .build();

        try {
            riakClient.execute(cmd);
            dumpOperation(data.getValue(), "UPSERT:RESULT - OK");
        } catch (Exception e) {
            dumpOperation(data.getValue(), "UPSERT FAILED");
            logger.error("UPSERT FAILED: UE", e);
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
            logger.error("DELETE FAILED: UE", e);
            return Status.ERROR;
        }

        return Status.OK;
    }
}
