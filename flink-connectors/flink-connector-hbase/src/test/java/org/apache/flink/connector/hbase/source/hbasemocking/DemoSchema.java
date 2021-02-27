/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase.source.hbasemocking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/** Adding DemoSchema to Hbase Test Cluster. */
public class DemoSchema {

    public static final String COLUMN_FAMILY_NAME = "info";
    public static final String DEFAULT_TABLE_NAME = "test-table";

    public final String tableName;

    public DemoSchema(String tableName) {
        this.tableName = tableName;
    }

    public DemoSchema() {
        this(DEFAULT_TABLE_NAME);
    }

    public void createSchema(Configuration hbaseConf) throws IOException {
        try (Admin admin = ConnectionFactory.createConnection(hbaseConf).getAdmin()) {
            TableName tableName = TableName.valueOf(this.tableName);
            if (!admin.tableExists(tableName)) {
                TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
                ColumnFamilyDescriptorBuilder cfBuilder =
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY_NAME));
                cfBuilder.setScope(1);
                tableBuilder.setColumnFamily(cfBuilder.build());
                admin.createTable(tableBuilder.build());
            }
        }
    }

    public void createMultiCFSchema(Configuration hbaseConf, int numColumnFamilies)
            throws IOException {
        try (Admin admin = ConnectionFactory.createConnection(hbaseConf).getAdmin()) {
            TableName tableName = TableName.valueOf(this.tableName);
            if (!admin.tableExists(tableName)) {
                TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
                for (int i = 0; i < numColumnFamilies; i++) {
                    ColumnFamilyDescriptorBuilder cfBuilder =
                            ColumnFamilyDescriptorBuilder.newBuilder(
                                    Bytes.toBytes(COLUMN_FAMILY_NAME + i));
                    cfBuilder.setScope(1);
                    tableBuilder.setColumnFamily(cfBuilder.build());
                }
                admin.createTable(tableBuilder.build());
            }
        }
    }
}
