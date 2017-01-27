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
		
		long timestamp;
		String host;
		String workerName;
		
    	if (key.startsWith("user")) {
			String k = key.replace("user", "");
			timestamp = Long.parseLong(k);
	    	host = hostname;
	    	workerName = "worker";
		} else {
			String[] parts = key.split(",");
			timestamp = Math.round((Long.parseLong(parts[0]) + 1) / config().threadCount());
	    	host = parts[1];
	    	workerName = parts[2];
		}
    	
    	final List<Cell> keyCells = Arrays.asList(new Cell(host), new Cell(workerName), Cell.newTimestamp(timestamp));
    
    	Fetch cmd = new Fetch.Builder(table, keyCells).build();
    	final QueryResult response;
    	try {
    		response = riakClient.execute(cmd);
    	} catch (Exception e) {
    		logger.error(e.getMessage());
    		return Status.ERROR;
    	}
    	
    	if ( response.getRowsCount() == 0 )
    	{
    		return Status.NOT_FOUND;
    	}
    	
    	return Status.OK;
    }

	@Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		long timestamp;
		String host;
		String workerName;
		
		if (startkey.startsWith("user")) {
			String k = startkey.replace("user", "");
			timestamp = Long.parseLong(k);
	    	host = hostname;
	    	workerName = "worker";
		} else {
			String[] parts = startkey.split(",");
			timestamp = Math.round((Long.parseLong(parts[0]) + 1) / config().threadCount());
	    	host = parts[1];
	    	workerName = parts[2];
		}
    	
        // Construct the query SQL 
        String query = String.format("SELECT * FROM %s " +
                " WHERE " +
                " host = '%s' " +
                " AND worker = '%s' " +
                " AND time >= %d AND time < %d",
                table, host, workerName, timestamp, timestamp+(recordcount*config().delta()));
                
        final Query cmd = new Query.Builder(query).build();

        final QueryResult response;
        try {
            response = riakClient.execute(cmd);
            //System.out.println(response.getRowsCount());
        } catch (Exception e) {
        	 logger.error(e.getMessage());
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
      List<Row> rows = new ArrayList<Row>();
      
      String[] keys = key.split(";");
      int batchSize = values.size() / keys.length;
      int currentValueIndex = 0;
      
      int rowCount = 1;
      
      for (String splitKey : keys) {
          long timestamp;
      		String host;
      		String workerName;
      		
      		if (splitKey.startsWith("user")) {
      			  String k = splitKey.replace("user", "");
      	    	timestamp = Long.parseLong(k) + 1;
      	    	host = hostname;
      	    	workerName = "worker";
      		} else {
      			String[] parts = splitKey.split(",");
      	    	timestamp = Long.parseLong(parts[0]);
      	    	if (timestamp == 0) {
      	    	  timestamp = 1;
      	    	}
      	    	host = parts[1];
      	    	workerName = parts[2];
      		}
      		
          // Build the row
          ArrayList<Cell> cells = new ArrayList<Cell>(batchSize + 3);
          
          cells.add(new Cell(host));
          cells.add(new Cell(workerName));
          cells.add(Cell.newTimestamp(timestamp));
          int trackValuesIndex = 0;
          String[] keyset = values.keySet().toArray(new String[values.keySet().size()]);
          
          for (int valuesIndex = currentValueIndex; valuesIndex < batchSize * rowCount; valuesIndex++)
      		{
            String value = values.get(keyset[valuesIndex]).toString();
      			cells.add(valueToCell(value));
      			trackValuesIndex = valuesIndex;
      		}

          rowCount++;
          currentValueIndex = trackValuesIndex+1;
          rows.add(new Row(cells));
      }
       
        final Store cmd = new Store.Builder(table)
                .withRows(rows)
                .build();

        try {
            riakClient.execute(cmd);
        } catch (Exception e) {
          logger.error(e.getMessage());
          return Status.ERROR;
        }

        return Status.OK;
    }

	private Cell valueToCell(String value) {     
      if (config().dataType().compareTo("integer") == 0) {
        return new Cell(Integer.parseInt(value));
      } else if (config().dataType().compareTo("double") == 0) {
        return new Cell(Double.parseDouble(value));
      } else {
        return new Cell(value);
      }
    }

  @Override
    public Status delete(String table, String key) {
		
		long timestamp;
		String host;
		String workerName;
		
    	if (key.startsWith("user")) {
			String k = key.replace("user", "");
			timestamp = Long.parseLong(k);
	    	host = hostname;
	    	workerName = "worker";
		} else {
			String[] parts = key.split(",");
			timestamp = Math.round((Long.parseLong(parts[0]) + 1) / config().threadCount());
	    	host = parts[1];
	    	workerName = parts[2];
		}
    	
    	// Build the key
    	ArrayList<Cell> cells = new ArrayList<Cell>(3);
    	cells.add(new Cell(host));
        cells.add(new Cell(workerName));
        cells.add(Cell.newTimestamp(timestamp));
        
        // Delete the key
        Delete delete = new Delete.Builder(table, cells).build();
        try {
			riakClient.execute(delete);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return Status.ERROR;
		}
        
        return Status.OK;
    }
}
