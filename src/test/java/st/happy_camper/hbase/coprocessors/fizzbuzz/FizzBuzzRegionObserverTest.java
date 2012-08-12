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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A test for {@link FizzBuzzRegionObserver}.
 * 
 * @author ueshin
 */
public class FizzBuzzRegionObserverTest {

    private static HBaseTestingUtility testingUtility = new HBaseTestingUtility();

    /**
     * Starts MiniCluster for tests.
     * 
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        testingUtility.startMiniCluster();
    }

    /**
     * Shutdowns MiniCluster.
     * 
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        testingUtility.shutdownMiniCluster();
    }

    /**
     * Creates a fizzbuzz table for tests.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        HTableDescriptor desc = new HTableDescriptor(FizzBuzzRegionObserver.TABLE_NAME);
        for(byte[] familyname : new byte[][] { FizzBuzzRegionObserver.NUM_COLUMN_FAMILY,
                FizzBuzzRegionObserver.FIZZ_COLUMN_FAMILY, FizzBuzzRegionObserver.BUZZ_COLUMN_FAMILY,
                FizzBuzzRegionObserver.FIZZBUZZ_COLUMN_FAMILY }) {
            HColumnDescriptor family = new HColumnDescriptor(familyname);
            family.setMaxVersions(Integer.MAX_VALUE);
            desc.addFamily(family);
        }
        testingUtility.getHBaseAdmin().createTable(desc);
    }

    /**
     * Deletes all tables created by tests.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        for(HTableDescriptor desc : testingUtility.getHBaseAdmin().listTables()) {
            testingUtility.deleteTable(desc.getName());
        }
    }

    /**
     * Test method for
     * {@link st.happy_camper.hbase.coprocessors.fizzbuzz.FizzBuzzRegionObserver#postPut(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Put, org.apache.hadoop.hbase.regionserver.wal.WALEdit, boolean)}
     * .
     * 
     * @throws Exception
     */
    @Test
    public void testPostPut() throws Exception {
        HTableDescriptor desc = new HTableDescriptor("target");
        desc.addFamily(new HColumnDescriptor("a"));
        desc.addFamily(new HColumnDescriptor("b"));
        desc.addFamily(new HColumnDescriptor("c"));
        desc.addFamily(new HColumnDescriptor("d"));

        Map<String, String> params = new HashMap<String, String>();
        params.put(FizzBuzzRegionObserver.CONF_FIZZBUZZ_TARGETS, "a:a a:aa b:b c: d");
        desc.addCoprocessor(FizzBuzzRegionObserver.class.getName(), null, Coprocessor.PRIORITY_USER, params);

        testingUtility.getHBaseAdmin().createTable(desc);

        HTable target = new HTable(testingUtility.getConfiguration(), "target");
        try {
            for(int i = 1; i <= 100; i++) {
                Put put = new Put(Bytes.toBytes(String.format("a%03d", i)), i);
                put.add(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes(i));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("aa3"));
                put.add(Bytes.toBytes("a"), Bytes.toBytes("aa"), Bytes.toBytes(3));
                target.put(put);
            }
            {
                // not target
                Put put = new Put(Bytes.toBytes("aaa5"));
                put.add(Bytes.toBytes("a"), Bytes.toBytes("aaa"), Bytes.toBytes(5));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("bb6"));
                put.add(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes(6));
                target.put(put);
            }
            {
                // not target
                Put put = new Put(Bytes.toBytes("bb9"));
                put.add(Bytes.toBytes("b"), Bytes.toBytes("bb"), Bytes.toBytes(9));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("c10"));
                put.add(Bytes.toBytes("c"), Bytes.toBytes(""), Bytes.toBytes(10));
                target.put(put);
            }
            {
                // not target
                Put put = new Put(Bytes.toBytes("c12"));
                put.add(Bytes.toBytes("c"), Bytes.toBytes("c"), Bytes.toBytes(12));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("d15"));
                put.add(Bytes.toBytes("d"), Bytes.toBytes(""), Bytes.toBytes(15));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("d18"));
                put.add(Bytes.toBytes("d"), Bytes.toBytes("d"), Bytes.toBytes(18));
                target.put(put);
            }
        }
        finally {
            target.close();
        }

        HTable fizzbuzz = new HTable(testingUtility.getConfiguration(), FizzBuzzRegionObserver.TABLE_NAME);
        try {
            ResultScanner scanner = fizzbuzz.getScanner(new Scan().setMaxVersions());
            try {
                Iterator<Result> itr = scanner.iterator();
                for(int i = 1; i <= 100; i++) {
                    Result result = itr.next();
                    if(i % 15 == 0) {
                        assertThat(result.getRow(), is(Bytes.add(Bytes.toBytes(i), Bytes.toBytes(":FizzBuzz"))));
                        assertThat(
                                result.getValue(
                                        FizzBuzzRegionObserver.FIZZBUZZ_COLUMN_FAMILY,
                                        Bytes.add(
                                                Bytes.add(Bytes.toBytes(6), Bytes.toBytes("target")), // table
                                                Bytes.toBytes((long) i), // timestamp
                                                Bytes.add(
                                                        Bytes.add(Bytes.toBytes((short) 4),
                                                                Bytes.toBytes(String.format("a%03d", i))), // rowkey
                                                        Bytes.add(Bytes.toBytes((short) 1), Bytes.toBytes("a")), // family
                                                        Bytes.add(Bytes.toBytes(1), Bytes.toBytes("a")) // qualifier
                                                ))), is(Bytes.toBytes(i))); // value
                        if(i == 15) {
                            assertThat(result.list().size(), is(2)); // d15
                        }
                        else {
                            assertThat(result.list().size(), is(1));
                        }
                    }
                    else if(i % 5 == 0) {
                        assertThat(result.getRow(), is(Bytes.add(Bytes.toBytes(i), Bytes.toBytes(":Buzz"))));
                        assertThat(
                                result.getValue(
                                        FizzBuzzRegionObserver.BUZZ_COLUMN_FAMILY,
                                        Bytes.add(
                                                Bytes.add(Bytes.toBytes(6), Bytes.toBytes("target")), // table
                                                Bytes.toBytes((long) i), // timestamp
                                                Bytes.add(
                                                        Bytes.add(Bytes.toBytes((short) 4),
                                                                Bytes.toBytes(String.format("a%03d", i))), // rowkey
                                                        Bytes.add(Bytes.toBytes((short) 1), Bytes.toBytes("a")), // family
                                                        Bytes.add(Bytes.toBytes(1), Bytes.toBytes("a")) // qualifier
                                                ))), is(Bytes.toBytes(i))); // value
                        if(i == 10) {
                            assertThat(result.list().size(), is(2)); // c10
                        }
                        else {
                            assertThat(result.list().size(), is(1));
                        }
                    }
                    else if(i % 3 == 0) {
                        assertThat(result.getRow(), is(Bytes.add(Bytes.toBytes(i), Bytes.toBytes(":Fizz"))));
                        assertThat(
                                result.getValue(
                                        FizzBuzzRegionObserver.FIZZ_COLUMN_FAMILY,
                                        Bytes.add(
                                                Bytes.add(Bytes.toBytes(6), Bytes.toBytes("target")), // table
                                                Bytes.toBytes((long) i), // timestamp
                                                Bytes.add(
                                                        Bytes.add(Bytes.toBytes((short) 4),
                                                                Bytes.toBytes(String.format("a%03d", i))), // rowkey
                                                        Bytes.add(Bytes.toBytes((short) 1), Bytes.toBytes("a")), // family
                                                        Bytes.add(Bytes.toBytes(1), Bytes.toBytes("a")) // qualifier
                                                ))), is(Bytes.toBytes(i))); // value
                        if(i == 3 || i == 6 || i == 18) {
                            assertThat(result.list().size(), is(2)); // aa3
                                                                     // bb6
                                                                     // d18
                        }
                        else {
                            assertThat(result.list().size(), is(1));
                        }
                    }
                    else {
                        assertThat(result.getRow(), is(Bytes.add(Bytes.toBytes(i), Bytes.toBytes(":" + i))));
                        assertThat(
                                result.getValue(
                                        FizzBuzzRegionObserver.NUM_COLUMN_FAMILY,
                                        Bytes.add(
                                                Bytes.add(Bytes.toBytes(6), Bytes.toBytes("target")), // table
                                                Bytes.toBytes((long) i), // timestamp
                                                Bytes.add(
                                                        Bytes.add(Bytes.toBytes((short) 4),
                                                                Bytes.toBytes(String.format("a%03d", i))), // rowkey
                                                        Bytes.add(Bytes.toBytes((short) 1), Bytes.toBytes("a")), // family
                                                        Bytes.add(Bytes.toBytes(1), Bytes.toBytes("a")) // qualifier
                                                ))), is(Bytes.toBytes(i))); // value
                    }
                }
                assertThat(itr.hasNext(), is(false));
            }
            finally {
                scanner.close();
            }
        }
        finally {
            fizzbuzz.close();
        }
    }
}
