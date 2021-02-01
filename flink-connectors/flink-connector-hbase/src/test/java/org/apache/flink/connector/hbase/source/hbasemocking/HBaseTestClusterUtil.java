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
import java.nio.file.Paths;
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
    private static String testFolder;

    public HBaseTestClusterUtil() {}

    public static void main(String[] args)
            throws ParserConfigurationException, SAXException, IOException {
        Arrays.asList(HdfsConstants.class.getDeclaredFields()).forEach(System.out::println);

        startCluster();
        DemoSchema schema = new DemoSchema();
        schema.createSchema(getConfig());
    }

    public static void startCluster() throws IOException {
        testFolder = Files.createTempDirectory(null).toString();

        // Fallback for windows users with space in user name, will not work if path contains space.
        if (testFolder.contains(" ")) testFolder = "/flink-hbase-test-data/";
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
        Paths.get(testFolder).toFile().delete();
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
