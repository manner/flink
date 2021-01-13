package org.apache.flink.connector.hbase.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer serializer} for {@link
 * HbaseSourceSplit}.
 */
public class HbaseSourceSplitSerializer implements SimpleVersionedSerializer<HbaseSourceSplit> {
    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(HbaseSourceSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.splitId());
            out.writeUTF(split.getHost());
            out.writeUTF(split.getTable());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public HbaseSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String id = in.readUTF();
            String host = in.readUTF();
            String regionId = in.readUTF();
            String table = in.readUTF();
            return new HbaseSourceSplit(
                    id, host, table, regionId, new Configuration()); // TODO find real configuration
        }
    }
}
