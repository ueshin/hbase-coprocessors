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
/**
 * <h1>FizzBuzzRegionObserver</h1>
 * 
 * <p>
 * This is an example Coprocessor to fizzbuzz numbers <code>put</code> into HBase tables.
 * </p>
 *
 * <h2>How to configure</h2>
 * 
 * <p>
 * Put the packaged jar file on HDFS and create table to put counts.
 * </p>
 * 
 * <p>
 * Configure <code>FizzBuzzRegionObserver</code> as follows:
 * </p>
 * 
 * <pre><code>
 * hbase> disable '&lt;tablename&gt;'
 * hbase> alter '&lt;tablename&gt;', METHOD => 'table_att',
 *   'coprocessor' => '[jarfile path]|st.happy_camper.hbase.coprocessors.fizzbuzz.FizzBuzzRegionObserver|[priority]|[kvs]'
 * hbase> enable '&lt;tablename&gt;'
 * </code></pre>
 * 
 * <h3>Params:</h3>
 * 
 * <ul>
 * <li>jarfile path: Path of the jar file. If it's null, the class will be loaded from default classloader.</li>
 * <li>priority: Priority</li>
 * <li>kvs: Key-value parameter pairs passed into the coprocessor.
 *   <ul>
 *     <li>targets: target columns to fizzbuzz. space-separete for multiple targets.</li>
 *   </ul>
 * </li>
 * </ul>
 * 
 * <h3>FizzBuzz table</h3>
 * 
 * <p>
 * This observer uses table as follows:
 * </p>
 * 
 * <pre><code>
 * create 'fizzbuzz',
 *   { NAME => 'num',      VERSIONS => JInteger::MAX_VALUE },
 *   { NAME => 'fizz',     VERSIONS => JInteger::MAX_VALUE },
 *   { NAME => 'buzz',     VERSIONS => JInteger::MAX_VALUE },
 *   { NAME => 'fizzbuzz', VERSIONS => JInteger::MAX_VALUE }
 * </code></pre>
 * 
 * @author ueshin
 */
package st.happy_camper.hbase.coprocessors.fizzbuzz;

