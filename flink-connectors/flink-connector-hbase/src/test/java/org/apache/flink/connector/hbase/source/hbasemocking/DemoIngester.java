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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** DemoIngester for adding entries to the Hbase Test Cluster. */
public class DemoIngester {
    private List<String> names;
    private List<String> domains;

    public static void main(String[] args) throws Exception {
        new DemoIngester().run();
    }

    public void run() throws Exception {
        Configuration conf = TestClusterStarter.getConfig();

        DemoSchema.createSchema(conf);

        final byte[] infoCf = Bytes.toBytes("info");

        // column qualifiers
        final byte[] nameCq = Bytes.toBytes("name");
        final byte[] emailCq = Bytes.toBytes("email");
        final byte[] ageCq = Bytes.toBytes("age");
        final byte[] payloadCq = Bytes.toBytes("payload");

        loadData();

        ObjectMapper jsonMapper = new ObjectMapper();

        Table htable =
                ConnectionFactory.createConnection(conf)
                        .getTable(TableName.valueOf("sep-user-demo"));

        while (true) {
            Thread.sleep(1000);
            byte[] rowkey = Bytes.toBytes(UUID.randomUUID().toString());
            Put put = new Put(rowkey);

            String name = pickName();
            String email = name.toLowerCase() + "@" + pickDomain();
            String age = String.valueOf((int) Math.ceil(Math.random() * 100));

            put.addColumn(infoCf, nameCq, Bytes.toBytes(name));
            put.addColumn(infoCf, emailCq, Bytes.toBytes(email));
            put.addColumn(infoCf, ageCq, Bytes.toBytes(age));

            MyPayload payload = new MyPayload();
            payload.setPartialUpdate(false);
            put.addColumn(infoCf, payloadCq, jsonMapper.writeValueAsBytes(payload));

            htable.put(put);
            System.out.println("Added row " + Bytes.toString(rowkey));
        }
    }

    private String pickName() {
        return names.get((int) Math.floor(Math.random() * names.size()));
    }

    private String pickDomain() {
        return domains.get((int) Math.floor(Math.random() * domains.size()));
    }

    private void loadData() throws IOException {
        // Names

        names = Arrays.asList("Jessica", "Laetitia", "Linus", "Wolfram");

        // Domains
        domains = new ArrayList<>();
        domains.add("gmail.com");
        domains.add("hotmail.com");
        domains.add("yahoo.com");
        domains.add("live.com");
        domains.add("ngdata.com");
    }

    /** Example Payload for inputting data in Hbase. */
    public static class MyPayload {
        boolean partialUpdate = false;

        public boolean isPartialUpdate() {
            return partialUpdate;
        }

        public void setPartialUpdate(boolean partialUpdate) {
            this.partialUpdate = partialUpdate;
        }
    }
}
