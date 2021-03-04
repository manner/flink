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

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** DemoIngester for adding entries to the Hbase Test Cluster. */
public class DemoIngester {
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private static final byte[] infoCf = Bytes.toBytes("info");

    // column qualifiers
    private static final byte[] nameCq = Bytes.toBytes("name");
    private static final byte[] emailCq = Bytes.toBytes("email");
    private static final byte[] ageCq = Bytes.toBytes("age");
    private static final byte[] payloadCq = Bytes.toBytes("payload");

    private final HBaseTestClusterUtil cluster;
    private List<String> names;
    private List<String> domains;
    private Table htable;

    private final String tableName;

    public DemoIngester(String tableName, HBaseTestClusterUtil cluster) {
        this.cluster = cluster;
        this.tableName = tableName;
        setup();
    }

    public void run() throws Exception {
        while (true) {
            Thread.sleep(1000);
            commitPut(createPut().f0);
        }
    }

    public void setup() {
        try {
            cluster.makeTable(tableName);
            loadData();
            htable =
                    ConnectionFactory.createConnection(cluster.getConfig())
                            .getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    public Tuple2<Put, String[]> createPut() throws JsonProcessingException {
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
        return Tuple2.of(
                put, new String[] {name, email, age, jsonMapper.writeValueAsString(payload)});
    }

    public Tuple2<Put, String> createOneColumnUniquePut() {
        String uuid = UUID.randomUUID().toString();
        byte[] rowkey = Bytes.toBytes(uuid);
        Put put = new Put(rowkey);
        put.addColumn(infoCf, nameCq, Bytes.toBytes(uuid));
        return Tuple2.of(put, uuid);
    }

    public void commitPut(Put put) {
        try {
            htable.put(put);
            System.out.println("Added row " + Bytes.toString(put.getRow()));
        } catch (IOException e) {
            e.printStackTrace();
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
