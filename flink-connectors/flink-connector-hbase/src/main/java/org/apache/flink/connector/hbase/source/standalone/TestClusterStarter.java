package org.apache.flink.connector.hbase.source.standalone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.FileOutputStream;
import java.io.IOException;

/** Bla. */
public class TestClusterStarter {

    public static final String CONFIG_PATH = "config.xml";
    private static MiniHBaseCluster cluster;
    private static Configuration hbaseConf;

    public static void main(String[] args) {
        startCluster();
    }

    public static void startCluster() {
        // System.setProperty("test.build.data.basedirectory", "foobarbaz");
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("tempusername"));
        // System.setProperty("user.name", "leonbein");

        hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("replication.stats.thread.period.seconds", 5);
        hbaseConf.setLong("replication.sleep.before.failover", 2000);
        hbaseConf.setInt("replication.source.maxretriesmultiplier", 10);
        hbaseConf.setBoolean("hbase.replication", true);

        // System.out.println(hbaseConf.getTrimmed("fs.defaultFS"));

        System.out.println(hbaseConf.get("hbase.fs.tmp.dir"));
        System.out.println(hbaseConf.get("hbase.rootdir"));
        System.out.println(hbaseConf.get("fs.defaultFS"));
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
            cluster = utility.startMiniCluster();
            System.out.println("==================");
            System.out.println(hbaseConf.get("hbase.fs.tmp.dir"));
            System.out.println(hbaseConf.get("hbase.rootdir"));
            System.out.println(hbaseConf.get("fs.defaultFS"));

            System.out.println("Basedir: " + System.getProperty("test.build.data.basedirectory"));
            int numRegionServers = utility.getHBaseCluster().getRegionServerThreads().size();
            System.out.println(numRegionServers);

            System.out.println(hbaseConf.get("hbase.zookeeper.quorum"));
            System.out.println(hbaseConf.get("hbase.zookeeper.property.clientPort"));
            System.out.println(hbaseConf.get("hbase.master.info.port"));
            System.out.println(hbaseConf.get("hbase.master.port"));
            System.out.println(hbaseConf.get("hbase.master.info.port"));
            System.out.println(hbaseConf.get("hbase.master.info.port"));

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
}
