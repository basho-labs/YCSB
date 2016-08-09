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
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 */
public class RiakTSClient extends AbstractRiakClient {
    
	@Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		// In RiakTS, a read is just a scan for a single key, so defer to the scan implementation
		return scan(table, key, 1, fields, null);
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        String k = startkey.replace("user", "");
    	Long lk = Long.parseLong(k) + 1;
    	
        // Construct the query SQL 
        String query = String.format("SELECT * FROM %s " +
                " WHERE " +
                " host = '%s' " +
                " AND worker = '%s' " +
                " AND time >= %d AND time < %d",
                table, hostname, "worker", lk, lk+recordcount);

        final Query cmd = new Query.Builder(query).build();

        final QueryResult response;
        try {
            response = riakClient.execute(cmd);
        } catch (Exception e) {
            return Status.ERROR;
        }

        if ( response.getRowsCount() == 0 )
        {
            return Status.NOT_FOUND;
        }
        
        return Status.OK;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        return insert(table, key, values);
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    	
    	// Build the timestamp
    	String k = key.replace("user", "");
    	Long lk = Long.parseLong(k) + 1;
    	
    	// Build the row
    	ArrayList<Cell> cells = new ArrayList<Cell>(values.size() + 3);
    	List<Row> rows = new ArrayList<Row>();
    	cells.add(new Cell(hostname));
        cells.add(new Cell("worker"));
        cells.add(Cell.newTimestamp(lk));
        for (int valuesIndex = 0; valuesIndex < values.size(); valuesIndex++)
		{
			String cKey = values.keySet().toArray()[valuesIndex].toString();
			cells.add(new Cell(values.get(cKey).toString()));
		}
        rows.add(new Row(cells));
       
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
		
		// Build the timestamp
    	String k = key.replace("user", "");
    	Long lk = Long.parseLong(k) + 1;
    	
    	// Build the key
    	ArrayList<Cell> cells = new ArrayList<Cell>(3);
    	cells.add(new Cell(hostname));
        cells.add(new Cell("worker"));
        cells.add(Cell.newTimestamp(lk));
        
        // Delete the key
        Delete delete = new Delete.Builder(table, cells).build();
        try {
			riakClient.execute(delete);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return Status.ERROR;
		}
        
        return Status.OK;
    }
}
