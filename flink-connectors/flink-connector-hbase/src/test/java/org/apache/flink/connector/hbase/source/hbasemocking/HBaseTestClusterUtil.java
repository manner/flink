package org.apache.flink.connector.hbase.source.hbasemocking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Bla. */
public class HBaseTestClusterUtil {

    static {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.WARN);
        ctx.updateLoggers();
    }

    public static final String CONFIG_PATH = "config.xml";
    private static MiniHBaseCluster cluster;
    private static Configuration hbaseConf;
    private static Path TEST_FOLDER;

    public HBaseTestClusterUtil() {
    }

    public static void main(String[] args)
            throws ParserConfigurationException, SAXException, IOException {
        Arrays.asList(HdfsConstants.class.getDeclaredFields()).forEach(System.out::println);

        startCluster();
        DemoSchema.createSchema(getConfig());
    }

    public static void startCluster() throws IOException {
        TEST_FOLDER = Files.createTempDirectory(null);
        // System.setProperty("test.build.data.basedirectory", "foobarbaz");
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("tempusername"));
        // System.setProperty("user.name", "leonbein");

        hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("replication.stats.thread.period.seconds", 5);
        hbaseConf.setLong("replication.sleep.before.failover", 2000);
        hbaseConf.setInt("replication.source.maxretriesmultiplier", 10);
        hbaseConf.setBoolean("hbase.replication", true);

        System.setProperty(HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY, TEST_FOLDER.toString());

        // System.out.println(hbaseConf.getTrimmed("fs.defaultFS"));

        System.out.println(hbaseConf.get("hbase.fs.tmp.dir"));
        System.out.println(hbaseConf.get("hbase.rootdir"));
        System.out.println("==================");
        // hbaseConf.set("hbase.fs.tmp.dir",	"hdfs://127.0.0.1:64771/user/Leon
        // Bein/test-data/d24ffa6b-b9a8-18b8-79d0-9430d327d07b/hbase-staging");
        // hbaseConf.set("hbase.rootdir", 		"hdfs://127.0.0.1:64771/user/Leon
        // Bein/test-data/d24ffa6b-b9a8-18b8-79d0-9430d327d07b");
        // hbaseConf.set("fs.defaultFS", 		"hdfs://127.0.0.1:64771/user/Leon
        // Bein/test-data/d24ffa6b-b9a8-18b8-79d0-9430d327d07b");

        // hbaseConf.setBoolean("hbase.cluster.distributed", false);
        // hbaseConf.set("hbase.rootdir", "file:///D:/hbase/");
        // hbaseConf.set("hbase.wal.dir", "file:///D:/hbase/wal/");
        // hbaseConf.set("hbase.zookeeper.property.dataDir", "D:/hbase/zookeeper/");
        HBaseTestingUtility utility = new HBaseTestingUtility(hbaseConf);
        System.out.println(utility.getDataTestDir().toString());
        try {
            cluster =
                    utility.startMiniCluster(
                            StartMiniClusterOption.builder().numRegionServers(3).build());
            System.out.println("==================");
            System.out.println(hbaseConf.get("hbase.fs.tmp.dir"));
            System.out.println(hbaseConf.get("hbase.rootdir"));
            System.out.println(hbaseConf.get("fs.defaultFS"));
            System.out.println(
                    "Base test directory: "
                            + hbaseConf.get(HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY));

            System.out.println("Basedir: " + System.getProperty("test.build.data.basedirectory"));
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
                System.out.println("Connected successfully");
            } catch (IOException e1) {
                e1.printStackTrace();
                Throwable e = e1;
                while (e.getCause() != null) {
                    e = e.getCause();
                }
                e.printStackTrace();
                System.exit(1);
            }

            hbaseConf.writeXml(new FileOutputStream(CONFIG_PATH));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void shutdownCluster() throws IOException {
        System.out.println("Shutting down test cluster");
        cluster.shutdown();
        cluster.waitUntilShutDown();
    }

    public static boolean isClusterAlreadyRunning() {
        try (Connection connection = ConnectionFactory.createConnection(getConfig())) {
            return true;
        } catch (ParserConfigurationException | IOException | SAXException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void clearReplicationPeers() {
        ReplicationPeerClearer.clearPeers();
    }

    public static Configuration getConfig()
            throws SAXException, IOException, ParserConfigurationException {
        Configuration hbaseConf = HBaseConfiguration.create();

        Document config =
                DocumentBuilderFactory.newInstance()
                        .newDocumentBuilder()
                        .parse(HBaseTestClusterUtil.CONFIG_PATH);
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
