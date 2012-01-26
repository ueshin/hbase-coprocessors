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
 * <h1>WordCountRegionObserver</h1>
 * 
 * <p>
 * This is an example Coprocessor to count words <code>put</code> into HBase tables.
 * </p>
 *
 * <h2>How to configure</h2>
 * 
 * <p>
 * Put the packaged jar file on HDFS and create table to put counts.
 * </p>
 * 
 * <p>
 * Configure <code>WordCountRegionObserver</code> as follows:
 * </p>
 * 
 * <pre><code>
 * hbase> disable '&lt;tablename&gt;'
 * hbase> alter '&lt;tablename&gt;', METHOD => 'table_att',
 *   'coprocessor' => '[jarfile path]|st.happy_camper.hbase.coprocessors.wordcount.WordCountRegionObserver|[priority]|[kvs]'
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
 *     <li>targets: target columns to count words. space-separete for multiple targets.</li>
 *     <li>table(optional): tablename to put counts.</li>
 *     <li>column(optional): column name of the count table.</li>
 *   </ul>
 * </li>
 * </ul>
 * 
 * <h3>Default count table</h3>
 * 
 * <p>
 * If you didn't specify the count table name or column name, this observer uses table like the following.
 * </p>
 * 
 * <pre><code>
 * hbase> create 'words', 'count'
 * </code></pre>
 */
package st.happy_camper.hbase.coprocessors.wordcount;

