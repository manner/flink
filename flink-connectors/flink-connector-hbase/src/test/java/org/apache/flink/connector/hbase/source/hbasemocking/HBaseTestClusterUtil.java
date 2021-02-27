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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Provides static access to a {@link MiniHBaseCluster} for testing. */
public class HBaseTestClusterUtil {

    public final String configPath = "config" + UUID.randomUUID() + ".xml";
    private MiniHBaseCluster cluster;
    private Configuration hbaseConf;
    private String testFolder;

    public HBaseTestClusterUtil() {}

    public static void main(String[] args)
            throws ParserConfigurationException, SAXException, IOException {
        Arrays.asList(HdfsConstants.class.getDeclaredFields()).forEach(System.out::println);
        HBaseTestClusterUtil hbaseTestClusterUtil = new HBaseTestClusterUtil();
        hbaseTestClusterUtil.startCluster();
        DemoSchema schema = new DemoSchema();
        schema.createSchema(hbaseTestClusterUtil.getConfig());
    }

    public void startCluster() throws IOException {
        System.out.println("Starting HBase test cluster ...");
        testFolder = Files.createTempDirectory(null).toString();

        // Fallback for windows users with space in user name, will not work if path contains space.
        if (testFolder.contains(" ")) {
            testFolder = "/flink-hbase-test-data/";
        }
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("tempusername"));

        hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("replication.stats.thread.period.seconds", 5);
        hbaseConf.setLong("replication.sleep.before.failover", 2000);
        hbaseConf.setInt("replication.source.maxretriesmultiplier", 10);
        hbaseConf.setBoolean("hbase.replication", true);

        System.setProperty(HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY, testFolder);
        System.out.println("Testfolder: " + testFolder);

        HBaseTestingUtility utility = new HBaseTestingUtility(hbaseConf);
        System.out.println(utility.getDataTestDir().toString());
        try {
            cluster =
                    utility.startMiniCluster(
                            StartMiniClusterOption.builder().numRegionServers(3).build());
            int numRegionServers = utility.getHBaseCluster().getRegionServerThreads().size();
            System.out.println(numRegionServers);

            System.out.println(hbaseConf.get("hbase.zookeeper.quorum"));
            System.out.println(hbaseConf.get("hbase.zookeeper.property.clientPort"));
            System.out.println(hbaseConf.get("hbase.master.info.port"));
            System.out.println(hbaseConf.get("hbase.master.port"));
            System.out.println(hbaseConf.get("hbase.master.info.port"));
            System.out.println(hbaseConf.get("hbase.master.info.port"));

            cluster.waitForActiveAndReadyMaster(30 * 1000);
            try {
                HBaseAdmin.available(hbaseConf);
                System.out.println("HBase test cluster up and running ...");
            } catch (IOException e1) {
                e1.printStackTrace();
                Throwable e = e1;
                while (e.getCause() != null) {
                    e = e.getCause();
                }
                e.printStackTrace();
                System.exit(1);
            }

            hbaseConf.writeXml(new FileOutputStream(configPath));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdownCluster()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        System.out.println("Shutting down HBase test cluster");
        clearTables();
        clearReplicationPeers();
        cluster.shutdown();
        new File(configPath).delete();
        CompletableFuture.runAsync(cluster::waitUntilShutDown).get(240, TimeUnit.SECONDS);
        Paths.get(testFolder).toFile().delete();
        System.out.println("HBase test cluster shut down");
    }

    public boolean isClusterAlreadyRunning() throws InterruptedException, ExecutionException {
        try {
            return CompletableFuture.supplyAsync(
                            () -> {
                                try (Connection connection =
                                        ConnectionFactory.createConnection(getConfig())) {
                                    return true;
                                } catch (ParserConfigurationException
                                        | IOException
                                        | SAXException e) {
                                    e.printStackTrace();
                                    return false;
                                }
                            })
                    .get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            System.out.println("Trying to connect to HBase test cluster timed out");
            e.printStackTrace(System.out);
            return false;
        }
    }

    public void clearTables() {
        try (Admin admin = ConnectionFactory.createConnection(getConfig()).getAdmin()) {
            for (TableDescriptor table : admin.listTableDescriptors()) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
        } catch (ParserConfigurationException | IOException | SAXException e) {
            e.printStackTrace();
        }
    }

    public void clearReplicationPeers() {
        try (Admin admin = ConnectionFactory.createConnection(getConfig()).getAdmin()) {
            for (ReplicationPeerDescription desc : admin.listReplicationPeers()) {
                System.out.println("==== " + desc.getPeerId() + " ====");
                System.out.println(desc);
                admin.removeReplicationPeer(desc.getPeerId());
            }
        } catch (SAXException | IOException | ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    public void commitPut(String tableName, Put put) {
        try (Table htable =
                ConnectionFactory.createConnection(getConfig())
                        .getTable(TableName.valueOf(tableName)); ) {
            htable.put(put);
            System.out.println("Added row " + Bytes.toString(put.getRow()));
        } catch (IOException | SAXException | ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    public Configuration getConfig()
            throws SAXException, IOException, ParserConfigurationException {
        Configuration hbaseConf = HBaseConfiguration.create();

        Document config =
                DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(configPath);
        NodeList nodes = config.getDocumentElement().getElementsByTagName("property");
        for (int i = 0; i < nodes.getLength(); i++) {
            Element e = (Element) nodes.item(i);
            hbaseConf.set(
                    e.getElementsByTagName("name").item(0).getTextContent(),
                    e.getElementsByTagName("value").item(0).getTextContent());
        }
        return hbaseConf;
    }
}
