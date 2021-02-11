package org.apache.flink.connector.hbase.source.split;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer serializer} for {@link
 * HBaseSourceSplit}.
 */
public class HBaseSourceSplitSerializer implements SimpleVersionedSerializer<HBaseSourceSplit> {
    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(HBaseSourceSplit split) throws IOException {
        System.out.println("Splitserializer.serialize");
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.splitId());
            out.writeUTF(split.getHost());
            out.writeUTF(split.getTable());
            out.writeUTF(split.getColumnFamily());
            out.writeLong(split.getFirstEventStamp().f0);
            out.writeInt(split.getFirstEventStamp().f1);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public HBaseSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        System.out.println("Splitserializer.deserialize");
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String id = in.readUTF();
            String host = in.readUTF();
            String table = in.readUTF();
            String columnFamily = in.readUTF();
            long firstTimestamp = in.readLong();
            int firstIndex = in.readInt();
            return new HBaseSourceSplit(
                    id,
                    host,
                    table,
                    columnFamily,
                    new Configuration(),
                    Tuple2.of(firstTimestamp, firstIndex)); // TODO find real configuration
        }
    }
}
