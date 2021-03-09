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

package org.apache.flink.connector.hbase.testutil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Provides static access to a {@link MiniHBaseCluster} for testing. */
public class HBaseTestClusterUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseTestClusterUtil.class);

    public static final String COLUMN_FAMILY_BASE = "info";
    public static final String DEFAULT_COLUMN_FAMILY = COLUMN_FAMILY_BASE + 0;
    public static final String QUALIFIER_BASE = "qualifier";
    public static final String DEFAULT_QUALIFIER = QUALIFIER_BASE + 0;

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
        hbaseTestClusterUtil.makeTable("tableName");
    }

    public void startCluster() throws IOException {
        LOG.info("Starting HBase test cluster ...");
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

        HBaseTestingUtility utility = new HBaseTestingUtility(hbaseConf);
        LOG.info("Testfolder: {}", utility.getDataTestDir().toString());
        try {
            cluster =
                    utility.startMiniCluster(
                            StartMiniClusterOption.builder().numRegionServers(3).build());
            int numRegionServers = utility.getHBaseCluster().getRegionServerThreads().size();
            LOG.info("Number of region servers: {}", numRegionServers);
            LOG.info(
                    "ZooKeeper client address: {}:{}",
                    hbaseConf.get("hbase.zookeeper.quorum"),
                    hbaseConf.get("hbase.zookeeper.property.clientPort"));
            LOG.info(
                    "Master port={}, web UI at port={}",
                    hbaseConf.get("hbase.master.port"),
                    hbaseConf.get("hbase.master.info.port"));

            cluster.waitForActiveAndReadyMaster(30 * 1000);
            HBaseAdmin.available(hbaseConf);
            LOG.info("HBase test cluster up and running ...");

            hbaseConf.writeXml(new FileOutputStream(configPath));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdownCluster()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        LOG.info("Shutting down HBase test cluster");
        clearTables();
        clearReplicationPeers();
        cluster.shutdown();
        new File(configPath).delete();
        CompletableFuture.runAsync(cluster::waitUntilShutDown).get(240, TimeUnit.SECONDS);
        Paths.get(testFolder).toFile().delete();
        LOG.info("HBase test cluster shut down");
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
            LOG.error("Trying to connect to HBase test cluster timed out", e);
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
            StringBuilder logMessage = new StringBuilder("Cleard existing replication peers:");
            for (ReplicationPeerDescription desc : admin.listReplicationPeers()) {
                logMessage.append("\n\t").append(desc.getPeerId()).append(" | ").append(desc);
                admin.removeReplicationPeer(desc.getPeerId());
            }
            LOG.info(logMessage.toString());
        } catch (SAXException | IOException | ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    public List<ReplicationPeerDescription> getReplicationPeers() {
        try (Admin admin = ConnectionFactory.createConnection(getConfig()).getAdmin()) {
            return admin.listReplicationPeers();
        } catch (SAXException | IOException | ParserConfigurationException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void makeTable(String tableName) {
        makeTable(tableName, 1);
    }

    /**
     * Creates a table for given name with given number of column families. Column family names
     * start with {@link HBaseTestClusterUtil#COLUMN_FAMILY_BASE} and are indexed, if more than one
     * is requested
     */
    public void makeTable(String tableName, int numColumnFamilies) {
        assert numColumnFamilies >= 1;
        try (Admin admin = ConnectionFactory.createConnection(getConfig()).getAdmin()) {
            TableName tableNameObj = TableName.valueOf(tableName);
            if (!admin.tableExists(tableNameObj)) {
                TableDescriptorBuilder tableBuilder =
                        TableDescriptorBuilder.newBuilder(tableNameObj);
                for (int i = 0; i < numColumnFamilies; i++) {
                    ColumnFamilyDescriptorBuilder cfBuilder =
                            ColumnFamilyDescriptorBuilder.newBuilder(
                                    Bytes.toBytes(COLUMN_FAMILY_BASE + i));
                    cfBuilder.setScope(1);
                    tableBuilder.setColumnFamily(cfBuilder.build());
                }
                admin.createTable(tableBuilder.build());
            }
        } catch (SAXException | IOException | ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    public void commitPut(String tableName, Put put) {
        try (Table htable =
                ConnectionFactory.createConnection(getConfig())
                        .getTable(TableName.valueOf(tableName))) {
            htable.put(put);
            LOG.info("Commited put to row {}", Bytes.toString(put.getRow()));
        } catch (IOException | SAXException | ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    public String put(String tableName, String value) {
        return put(tableName, 1, value);
    }

    public void delete(String tableName, String rowKey, String columnFamily, String qualifier) {
        try (Table htable =
                ConnectionFactory.createConnection(getConfig())
                        .getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(rowKey.getBytes());
            delete.addColumn(columnFamily.getBytes(), qualifier.getBytes());
            htable.delete(delete);
            LOG.info("Deleted row {}", rowKey);
        } catch (IOException | SAXException | ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    public String put(String tableName, int numColumnFamilies, String... values) {
        assert numColumnFamilies >= 1;
        assert values.length >= numColumnFamilies;
        try (Table htable =
                ConnectionFactory.createConnection(getConfig())
                        .getTable(TableName.valueOf(tableName))) {

            String rowKey = UUID.randomUUID().toString();
            Put put = new Put(rowKey.getBytes());
            int index = 0;
            for (int cf = 0; cf < numColumnFamilies; cf++) {
                int cq = 0;
                for (; index + cq < values.length * (cf + 1) / numColumnFamilies; cq++) {
                    put.addColumn(
                            (COLUMN_FAMILY_BASE + cf).getBytes(),
                            (QUALIFIER_BASE + cq).getBytes(),
                            values[index + cq].getBytes());
                }
                index += cq;
            }
            htable.put(put);
            LOG.info("Added row " + rowKey);
            return rowKey;
        } catch (IOException | SAXException | ParserConfigurationException e) {
            e.printStackTrace();
            return null;
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
