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
package st.happy_camper.hbase.coprocessors.fizzbuzz;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
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
 * A coprocessor to FizzBuzz.
 * 
 * @author ueshin
 */
public class FizzBuzzRegionObserver extends BaseRegionObserver {

    /**
     * Configuration key for target columns.
     */
    public static final String CONF_FIZZBUZZ_TARGETS = "targets";

    /**
     * A tablename fo the FizzBuzz table.
     */
    public static final byte[] TABLE_NAME = Bytes.toBytes("fizzbuzz");

    /**
     * A column-family name for numbers of the FizzBuzz table.
     */
    public static final byte[] NUM_COLUMN_FAMILY = Bytes.toBytes("num");

    /**
     * A column-family name for Fizz of the FizzBuzz table.
     */
    public static final byte[] FIZZ_COLUMN_FAMILY = Bytes.toBytes("fizz");

    /**
     * A column-family name for Buzz of the FizzBuzz table.
     */
    public static final byte[] BUZZ_COLUMN_FAMILY = Bytes.toBytes("buzz");

    /**
     * A column-family name for FizzBuzz of the FizzBuzz table.
     */
    public static final byte[] FIZZBUZZ_COLUMN_FAMILY = Bytes.toBytes("fizzbuzz");

    /**
     * A row-key suffix for numbers.
     */
    public static final byte[] NUM_SUFFIX = Bytes.toBytes(":");

    /**
     * A row-key suffix for Fizz.
     */
    public static final byte[] FIZZ_SUFFIX = Bytes.toBytes(":Fizz");

    /**
     * A row-key suffix for Buzz.
     */
    public static final byte[] BUZZ_SUFFIX = Bytes.toBytes(":Buzz");

    /**
     * A row-key suffix for FizzBuzz.
     */
    public static final byte[] FIZZBUZZ_SUFFIX = Bytes.toBytes(":FizzBuzz");

    private List<Pair<byte[], byte[]>> targets = new ArrayList<Pair<byte[], byte[]>>();

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        Configuration conf = e.getConfiguration();
        String targetString = conf.get(CONF_FIZZBUZZ_TARGETS);
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
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL)
            throws IOException {
        byte[] table = e.getEnvironment().getRegion().getTableDesc().getName();
        HTableInterface fizzbuzz = e.getEnvironment().getTable(TABLE_NAME);
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
                    List<Put> puts = new ArrayList<Put>(kvs.size());
                    for(KeyValue kv : kvs) {
                        puts.add(newPut(table, kv));
                    }
                    fizzbuzz.put(puts);
                }
            }
        }
        finally {
            fizzbuzz.close();
        }
    }

    private Put newPut(byte[] table, KeyValue kv) {
        int num = Bytes.toInt(kv.getValue());

        byte[] family, suffix;
        if(num % 15 == 0) {
            family = FIZZBUZZ_COLUMN_FAMILY;
            suffix = FIZZBUZZ_SUFFIX;
        }
        else if(num % 5 == 0) {
            family = BUZZ_COLUMN_FAMILY;
            suffix = BUZZ_SUFFIX;
        }
        else if(num % 3 == 0) {
            family = FIZZ_COLUMN_FAMILY;
            suffix = FIZZ_SUFFIX;
        }
        else {
            family = NUM_COLUMN_FAMILY;
            suffix = Bytes.add(NUM_SUFFIX, Bytes.toBytes(Integer.toString(num)));
        }

        byte[] kvTable = Bytes.add(Bytes.toBytes(table.length), table);
        byte[] kvRow = Bytes.add(Bytes.toBytes(kv.getRowLength()), kv.getRow());
        byte[] kvFamily = Bytes.add(Bytes.toBytes(kv.getFamilyLength()), kv.getFamily());
        byte[] kvQualifier = Bytes.add(Bytes.toBytes(kv.getQualifierLength()), kv.getQualifier());

        Put put = new Put(Bytes.add(kv.getValue(), suffix), kv.getTimestamp());
        put.add(family, Bytes.add(kvTable, Bytes.toBytes(kv.getTimestamp()), Bytes.add(kvRow, kvFamily, kvQualifier)),
                kv.getValue());

        return put;
    }
}
