/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;

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

    public void main(String[] args) throws Exception {
        Configuration conf = HBaseTestClusterUtil.getConfig();
        createSchema(conf);
    }

    public void createSchema(Configuration hbaseConf) throws IOException {
        Admin admin = ConnectionFactory.createConnection(hbaseConf).getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            HColumnDescriptor infoCf = new HColumnDescriptor(COLUMN_FAMILY_NAME);
            infoCf.setScope(1);
            tableDescriptor.addFamily(infoCf);

            admin.createTable(tableDescriptor);
        }
        admin.close();
    }
}
