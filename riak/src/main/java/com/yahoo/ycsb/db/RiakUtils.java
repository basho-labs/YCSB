/*
 * Copyright 2014 Basho Technologies, Inc.
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

import static com.google.common.base.Preconditions.*;
import static com.google.common.collect.Maps.newHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;

/**
 * @author Basho Technologies, Inc.
 * @author Sergey Galkin <srggal at gmail dot com>
 */
final class RiakUtils {

    private RiakUtils() {
        super();
    }


    static byte[] toBytes(final int anInteger) {
        byte[] aResult = new byte[4];

        aResult[0] = (byte) (anInteger >> 24);
        aResult[1] = (byte) (anInteger >> 16);
        aResult[2] = (byte) (anInteger >> 8);
        aResult[3] = (byte) (anInteger /* >> 0 */);

        return aResult;
    }


    static int fromBytes(final byte[] aByteArray) {
        checkArgument(aByteArray.length == 4);
        return aByteArray[0] << 24 | (aByteArray[1] & 0xFF) << 16
                | (aByteArray[2] & 0xFF) << 8 | (aByteArray[3] & 0xFF);
    }

    
    static void deserializeTable(final RiakObject aRiakObject, final HashMap<String, ByteIterator> theResult) {
        deserializeTable(aRiakObject.getValue().getValue(), theResult);
    }


    static void deserializeTable(final byte[] aValue, final Map<String, ByteIterator> theResult) {
        final ByteArrayInputStream anInputStream = new ByteArrayInputStream(aValue);

        try {

            byte[] aSizeBuffer = new byte[4];
            while (anInputStream.available() > 0) {

                anInputStream.read(aSizeBuffer);
                final int aColumnNameLength = fromBytes(aSizeBuffer);

                final byte[] aColumnNameBuffer = new byte[aColumnNameLength];
                anInputStream.read(aColumnNameBuffer);

                anInputStream.read(aSizeBuffer);
                final int aColumnValueLength = fromBytes(aSizeBuffer);

                final byte[] aColumnValue = new byte[aColumnValueLength];
                anInputStream.read(aColumnValue);

                theResult.put(new String(aColumnNameBuffer),
                        new ByteArrayByteIterator(aColumnValue));

            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            close(anInputStream);
        }
    }

    
    static void close(final InputStream anInputStream) {
        try {
            anInputStream.close();
        } catch (IOException e) {
            // Ignore exception ...
        }
    }

    
    static void close(final OutputStream anOutputStream) {
        try {
            anOutputStream.close();
        } catch (IOException e) {
            // Ignore exception ...
        }
    }

    
    static byte[] serializeTable(Map<String, ByteIterator> aTable) {
        final ByteArrayOutputStream anOutputStream = new ByteArrayOutputStream();

        try {
            final Set<Map.Entry<String, ByteIterator>> theEntries = aTable
                    .entrySet();
            for (final Map.Entry<String, ByteIterator> anEntry : theEntries) {

                final byte[] aColumnName = anEntry.getKey().getBytes();

                anOutputStream.write(toBytes(aColumnName.length));
                anOutputStream.write(aColumnName);

                final byte[] aColumnValue = anEntry.getValue().toArray();

                anOutputStream.write(toBytes(aColumnValue.length));
                anOutputStream.write(aColumnValue);
            }
            return anOutputStream.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            close(anOutputStream);
        }
    }

    
    static <K, V> Map<K, V> merge(final Map<K, V> aMap, final Map<K, V> theUpdatedMap) {
        checkNotNull(aMap);
        checkNotNull(theUpdatedMap);

        final Map<K, V> theResult = newHashMap(aMap);

        for (Map.Entry<K, V> aColumn : theUpdatedMap.entrySet()) {
            theResult.put(aColumn.getKey(), aColumn.getValue());
        }

        return theResult;
    }
    
	
	static Long getKeyAsLong(String key) {
		String key_string = key.replace("user", "").replaceFirst("^0*", "");
    	return Long.parseLong( key_string );
	}

    static final int TS_NUMBER_OF_INTERNAL_COLUMNS = 4;

    static Row asTSRow(String key, Map<String, ByteIterator> values) {
        final String parts[] = key.split(",");

        if (parts.length != TS_NUMBER_OF_INTERNAL_COLUMNS){
            throw new IllegalStateException("Wrong Key format, expected key with timestamp, original key, host and workerId");
        }

        final long timestamp = Long.parseLong(parts[0]);
        final String originalKey = parts[1];
        final String host = parts[2];
        final String worker = parts[3];

        final int cellCount = values.size() + TS_NUMBER_OF_INTERNAL_COLUMNS;
        final ArrayList<Cell> cells = new ArrayList<Cell>(cellCount);

        cells.add(new Cell(host));
        cells.add(new Cell(worker));
        cells.add(Cell.newTimestamp(timestamp));


        if (!values.isEmpty()){
            cells.add(new Cell(originalKey));

            final Iterator<Map.Entry<String, ByteIterator>> iterator = values.entrySet().iterator();
            for (int i=TS_NUMBER_OF_INTERNAL_COLUMNS; i<cellCount; ++i) {

                final Map.Entry<String, ByteIterator> e = iterator.next();
                cells.add(new Cell(e.getValue().toString()));
            }
        }
        return new Row(cells);
    }

    static Map.Entry<List<ColumnDescription>, Row> asTSRowWithColumns(String key, Map<String, ByteIterator> values) {
        ArrayList<ColumnDescription> columns = new ArrayList<ColumnDescription>(values.size() + TS_NUMBER_OF_INTERNAL_COLUMNS);
        columns.add(new ColumnDescription("host", ColumnDescription.ColumnType.VARCHAR));
        columns.add(new ColumnDescription("worker", ColumnDescription.ColumnType.VARCHAR));
        columns.add(new ColumnDescription("time", ColumnDescription.ColumnType.TIMESTAMP));
        columns.add(new ColumnDescription("okey", ColumnDescription.ColumnType.VARCHAR));

        for (String k: values.keySet()){
            columns.add(new ColumnDescription(k, ColumnDescription.ColumnType.VARCHAR));
        }

        return new AbstractMap.SimpleImmutableEntry<List<ColumnDescription>, Row>(columns, asTSRow(key, values));
    }

    static Vector<HashMap<String, ByteIterator>> asSYCSBResults(QueryResult queryResult) {
        final Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String, com.yahoo.ycsb.ByteIterator>>(queryResult.getRows().size());

        final int columnsInTotal = queryResult.getColumnDescriptions().size();
        final int columnCount = columnsInTotal - TS_NUMBER_OF_INTERNAL_COLUMNS;
        final List<ColumnDescription> columns = queryResult.getColumnDescriptions();

        for (Row row: queryResult.getRows()){
            final HashMap<String, ByteIterator> m = new HashMap<String, ByteIterator>(columnCount);
            for (int i=TS_NUMBER_OF_INTERNAL_COLUMNS; i<columnsInTotal; ++i ){
                final Cell c = row.getCells().get(i);
                m.put(columns.get(i).getName(), new ByteArrayByteIterator(c.getVarcharValue().unsafeGetValue()));
            }

            result.add(m);
        }
        return result;
    }

}
