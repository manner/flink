package org.apache.flink.connector.hbase.source.hbaseMocking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;

/** Bla. */
public class ReplicationPeerClearer {

    public static void main(String[] args) {
        clearPeers();
    }

    public static void clearPeers() {
        try {
            Configuration conf = TestClusterStarter.getConfig();
            try (Admin admin = ConnectionFactory.createConnection(conf).getAdmin()) {
                for (ReplicationPeerDescription desc : admin.listReplicationPeers()) {
                    System.out.println("==== " + desc.getPeerId() + " ====");
                    System.out.println(desc);
                    admin.removeReplicationPeer(desc.getPeerId());
                }
            }

        } catch (SAXException | IOException | ParserConfigurationException e) {
            e.printStackTrace();
        }
    }
}
