/*
 * Copyright 2012 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package st.happy_camper.hbase.coprocessors.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * A coprocessor to count words of contents put into target columns.
 * 
 * @author ueshin
 */
public class WordCountRegionObserver extends BaseRegionObserver {

    /**
     * Configuration key for target columns.
     */
    public static final String CONF_COUNT_TARGETS = "targets";

    /**
     * Configuration key for the count table name.
     */
    public static final String CONF_COUNT_TABLE_NAME = "table";

    /**
     * Configuration key for column of the count table.
     */
    public static final String CONF_COUNT_COLUMN = "column";

    /**
     * Default tablename of the count table.
     */
    public static final String DEFAULT_COUNT_TABLE_NAME = "words";

    /**
     * byte[] of default tablename of the count table.
     */
    public static final byte[] DEFAULT_COUNT_TABLE_NAME_BYTES = Bytes.toBytes(DEFAULT_COUNT_TABLE_NAME);

    /**
     * Default column-family name of the count table.
     */
    public static final String DEFAULT_COUNT_COLUMN_FAMILY = "count";

    /**
     * byte[] of default column-family name of the count table.
     */
    public static final byte[] DEFAULT_COUNT_COLUMN_FAMILY_BYTES = Bytes.toBytes(DEFAULT_COUNT_COLUMN_FAMILY);

    /**
     * Default column-qualifier name of the count table.
     */
    public static final String DEFAULT_COUNT_QUALIFIER = "";

    /**
     * byte[] of default column-qualifier name of the count table.
     */
    public static final byte[] DEFAULT_COUNT_COLUMN_QUALIFIER_BYTES = Bytes.toBytes(DEFAULT_COUNT_QUALIFIER);

    private byte[] tableName;

    private byte[] columnFamily;

    private byte[] qualifier;

    private List<Pair<byte[], byte[]>> targets = new ArrayList<Pair<byte[], byte[]>>();

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        Configuration conf = e.getConfiguration();
        tableName = Bytes.toBytes(conf.get(CONF_COUNT_TABLE_NAME, DEFAULT_COUNT_TABLE_NAME));
        String[] column = conf.get(CONF_COUNT_COLUMN, DEFAULT_COUNT_COLUMN_FAMILY).split(":", 2);
        columnFamily = Bytes.toBytes(column[0]);
        qualifier = column.length > 1 ? Bytes.toBytes(column[1]) : HConstants.EMPTY_BYTE_ARRAY;

        String targetString = conf.get(CONF_COUNT_TARGETS);
        if(targetString != null) {
            for(String pair : targetString.split(" +")) {
                if(!pair.isEmpty()) {
                    String[] parts = pair.split(":", 2);
                    targets.add(new Pair<byte[], byte[]>(Bytes.toBytes(parts[0]), parts.length > 1 ? Bytes
                            .toBytes(parts[1]) : null));
                }
            }
        }
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, boolean writeToWAL)
            throws IOException {
        HTableInterface table = c.getEnvironment().getTable(tableName);
        try {
            for(Pair<byte[], byte[]> target : targets) {
                List<KeyValue> kvs;
                if(target.getSecond() == null) {
                    kvs = put.getFamilyMap().get(target.getFirst());
                }
                else {
                    kvs = put.get(target.getFirst(), target.getSecond());
                }
                if(kvs != null) {
                    for(KeyValue kv : kvs) {
                        for(String word : Bytes.toString(kv.getValue()).split("\\W+")) {
                            table.incrementColumnValue(Bytes.toBytes(word), columnFamily, qualifier, 1);
                        }
                    }
                }
            }
        }
        finally {
            table.close();
        }
    }

}
