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
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestCase for WordCountRegionObserver.
 * 
 * @author ueshin
 */
public class WordCountReginObserverTest {

    private static HBaseTestingUtility testingUtility = new HBaseTestingUtility();

    /**
     * start MiniCluster for tests.
     * 
     * @throws Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        testingUtility.startMiniCluster();
    }

    /**
     * shutdown MiniCluster.
     * 
     * @throws Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        testingUtility.shutdownMiniCluster();
    }

    /**
     * create a default count table for tests.
     * 
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        testingUtility.createTable(WordCountRegionObserver.DEFAULT_COUNT_TABLE_NAME_BYTES,
                WordCountRegionObserver.DEFAULT_COUNT_COLUMN_FAMILY_BYTES);
    }

    /**
     * delete all tables created by tests.
     * 
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        for(HTableDescriptor desc : testingUtility.getHBaseAdmin().listTables()) {
            testingUtility.deleteTable(desc.getName());
        }
    }

    /**
     * test if WordCountRegionObserver works or not with the default count
     * table.
     * 
     * @throws Exception
     */
    @Test
    public void testDefaultCountTable() throws Exception {
        HTableDescriptor desc = new HTableDescriptor("target");
        desc.addFamily(new HColumnDescriptor("a"));
        desc.addFamily(new HColumnDescriptor("b"));
        desc.addFamily(new HColumnDescriptor("c"));
        desc.addFamily(new HColumnDescriptor("d"));

        Map<String, String> params = new HashMap<String, String>();
        params.put(WordCountRegionObserver.CONF_COUNT_TARGETS, "a:a a:aa b:b c: d");
        desc.addCoprocessor(WordCountRegionObserver.class.getName(), null, Coprocessor.PRIORITY_USER, params);

        testingUtility.getHBaseAdmin().createTable(desc);

        HTable target = new HTable(testingUtility.getConfiguration(), "target");
        try {
            {
                Put put = new Put(Bytes.toBytes("aa"));
                put.add(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a a"));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("aa"));
                put.add(Bytes.toBytes("a"), Bytes.toBytes("aa"), Bytes.toBytes("a a a"));
                target.put(put);
            }
            {
                // no count
                Put put = new Put(Bytes.toBytes("aaa"));
                put.add(Bytes.toBytes("a"), Bytes.toBytes("aaa"), Bytes.toBytes("a a a a"));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("bb"));
                put.add(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes("b b"));
                target.put(put);
            }
            {
                // no count
                Put put = new Put(Bytes.toBytes("bb"));
                put.add(Bytes.toBytes("b"), Bytes.toBytes("bb"), Bytes.toBytes("b b b"));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("c"));
                put.add(Bytes.toBytes("c"), Bytes.toBytes(""), Bytes.toBytes("c"));
                target.put(put);
            }
            {
                // no count
                Put put = new Put(Bytes.toBytes("c"));
                put.add(Bytes.toBytes("c"), Bytes.toBytes("c"), Bytes.toBytes("c c"));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("d"));
                put.add(Bytes.toBytes("d"), Bytes.toBytes(""), Bytes.toBytes("d"));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("d"));
                put.add(Bytes.toBytes("d"), Bytes.toBytes("d"), Bytes.toBytes("d d"));
                target.put(put);
            }
        }
        finally {
            target.close();
        }

        HTable count = new HTable(testingUtility.getConfiguration(),
                WordCountRegionObserver.DEFAULT_COUNT_TABLE_NAME_BYTES);
        try {
            ResultScanner scanner = count.getScanner(WordCountRegionObserver.DEFAULT_COUNT_COLUMN_FAMILY_BYTES,
                    WordCountRegionObserver.DEFAULT_COUNT_COLUMN_QUALIFIER_BYTES);
            try {
                Iterator<Result> itr = scanner.iterator();
                {
                    Result result = itr.next();
                    assertThat(result.getRow(), is(Bytes.toBytes("a")));
                    assertThat(result.getValue(WordCountRegionObserver.DEFAULT_COUNT_COLUMN_FAMILY_BYTES,
                            WordCountRegionObserver.DEFAULT_COUNT_COLUMN_QUALIFIER_BYTES), is(Bytes.toBytes(5L)));
                }
                {
                    Result result = itr.next();
                    assertThat(result.getRow(), is(Bytes.toBytes("b")));
                    assertThat(result.getValue(WordCountRegionObserver.DEFAULT_COUNT_COLUMN_FAMILY_BYTES,
                            WordCountRegionObserver.DEFAULT_COUNT_COLUMN_QUALIFIER_BYTES), is(Bytes.toBytes(2L)));
                }
                {
                    Result result = itr.next();
                    assertThat(result.getRow(), is(Bytes.toBytes("c")));
                    assertThat(result.getValue(WordCountRegionObserver.DEFAULT_COUNT_COLUMN_FAMILY_BYTES,
                            WordCountRegionObserver.DEFAULT_COUNT_COLUMN_QUALIFIER_BYTES), is(Bytes.toBytes(1L)));
                }
                {
                    Result result = itr.next();
                    assertThat(result.getRow(), is(Bytes.toBytes("d")));
                    assertThat(result.getValue(WordCountRegionObserver.DEFAULT_COUNT_COLUMN_FAMILY_BYTES,
                            WordCountRegionObserver.DEFAULT_COUNT_COLUMN_QUALIFIER_BYTES), is(Bytes.toBytes(3L)));
                }
                assertThat(itr.hasNext(), is(false));
            }
            finally {
                scanner.close();
            }
        }
        finally {
            count.close();
        }
    }

    /**
     * test if WordCountRegionObserver works or not with another count table
     * specified in the configuration.
     * 
     * @throws Exception
     */
    @Test
    public void testAnotherCountTable() throws Exception {
        testingUtility.createTable(Bytes.toBytes("another"), Bytes.toBytes("cnt"));

        HTableDescriptor desc = new HTableDescriptor("target");
        desc.addFamily(new HColumnDescriptor("a"));
        desc.addFamily(new HColumnDescriptor("b"));
        desc.addFamily(new HColumnDescriptor("c"));
        desc.addFamily(new HColumnDescriptor("d"));

        Map<String, String> params = new HashMap<String, String>();
        params.put(WordCountRegionObserver.CONF_COUNT_TABLE_NAME, "another");
        params.put(WordCountRegionObserver.CONF_COUNT_COLUMN, "cnt:words");
        params.put(WordCountRegionObserver.CONF_COUNT_TARGETS, "a:a a:aa b:b c: d");
        desc.addCoprocessor(WordCountRegionObserver.class.getName(), null, Coprocessor.PRIORITY_USER, params);

        testingUtility.getHBaseAdmin().createTable(desc);

        HTable target = new HTable(testingUtility.getConfiguration(), "target");
        try {
            {
                Put put = new Put(Bytes.toBytes("aa"));
                put.add(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a a"));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("aa"));
                put.add(Bytes.toBytes("a"), Bytes.toBytes("aa"), Bytes.toBytes("a a a"));
                target.put(put);
            }
            {
                // no count
                Put put = new Put(Bytes.toBytes("aaa"));
                put.add(Bytes.toBytes("a"), Bytes.toBytes("aaa"), Bytes.toBytes("a a a a"));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("bb"));
                put.add(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes("b b"));
                target.put(put);
            }
            {
                // no count
                Put put = new Put(Bytes.toBytes("bb"));
                put.add(Bytes.toBytes("b"), Bytes.toBytes("bb"), Bytes.toBytes("b b b"));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("c"));
                put.add(Bytes.toBytes("c"), Bytes.toBytes(""), Bytes.toBytes("c"));
                target.put(put);
            }
            {
                // no count
                Put put = new Put(Bytes.toBytes("c"));
                put.add(Bytes.toBytes("c"), Bytes.toBytes("c"), Bytes.toBytes("c c"));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("d"));
                put.add(Bytes.toBytes("d"), Bytes.toBytes(""), Bytes.toBytes("d"));
                target.put(put);
            }
            {
                Put put = new Put(Bytes.toBytes("d"));
                put.add(Bytes.toBytes("d"), Bytes.toBytes("d"), Bytes.toBytes("d d"));
                target.put(put);
            }
        }
        finally {
            target.close();
        }

        HTable another = new HTable(testingUtility.getConfiguration(), "another");
        try {
            ResultScanner scanner = another.getScanner(Bytes.toBytes("cnt"), Bytes.toBytes("words"));
            try {
                Iterator<Result> itr = scanner.iterator();
                {
                    Result result = itr.next();
                    assertThat(result.getRow(), is(Bytes.toBytes("a")));
                    assertThat(result.getValue(Bytes.toBytes("cnt"), Bytes.toBytes("words")), is(Bytes.toBytes(5L)));
                }
                {
                    Result result = itr.next();
                    assertThat(result.getRow(), is(Bytes.toBytes("b")));
                    assertThat(result.getValue(Bytes.toBytes("cnt"), Bytes.toBytes("words")), is(Bytes.toBytes(2L)));
                }
                {
                    Result result = itr.next();
                    assertThat(result.getRow(), is(Bytes.toBytes("c")));
                    assertThat(result.getValue(Bytes.toBytes("cnt"), Bytes.toBytes("words")), is(Bytes.toBytes(1L)));
                }
                {
                    Result result = itr.next();
                    assertThat(result.getRow(), is(Bytes.toBytes("d")));
                    assertThat(result.getValue(Bytes.toBytes("cnt"), Bytes.toBytes("words")), is(Bytes.toBytes(3L)));
                }
                assertThat(itr.hasNext(), is(false));
            }
            finally {
                scanner.close();
            }
        }
        finally {
            another.close();
        }

        HTable count = new HTable(testingUtility.getConfiguration(),
                WordCountRegionObserver.DEFAULT_COUNT_TABLE_NAME_BYTES);
        try {
            ResultScanner scanner = count.getScanner(WordCountRegionObserver.DEFAULT_COUNT_COLUMN_FAMILY_BYTES,
                    WordCountRegionObserver.DEFAULT_COUNT_COLUMN_QUALIFIER_BYTES);
            try {
                assertThat(scanner.iterator().hasNext(), is(false));
            }
            finally {
                scanner.close();
            }
        }
        finally {
            count.close();
        }
    }

}
