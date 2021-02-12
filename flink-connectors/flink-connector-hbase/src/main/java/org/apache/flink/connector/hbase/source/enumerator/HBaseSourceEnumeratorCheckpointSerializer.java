package org.apache.flink.connector.hbase.source.enumerator;

import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Checkpoint serializer for hBase source. (De-)Serializes the collection of splits */
public class HBaseSourceEnumeratorCheckpointSerializer
        implements SimpleVersionedSerializer<Collection<HBaseSourceSplit>> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(Collection<HBaseSourceSplit> checkpointState) throws IOException {
        System.out.println("SERIALIZE");
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(checkpointState.size());

            HBaseSourceSplitSerializer splitSerializer = new HBaseSourceSplitSerializer();
            for (HBaseSourceSplit split : checkpointState) {
                byte[] serializedSplit = splitSerializer.serialize(split);
                out.write(serializedSplit.length);
                out.write(serializedSplit);
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public Collection<HBaseSourceSplit> deserialize(int version, byte[] serialized)
            throws IOException {
        System.out.println("DESERIALIZE");
        List<HBaseSourceSplit> checkPoint = new ArrayList<>();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            HBaseSourceSplitSerializer splitSerializer = new HBaseSourceSplitSerializer();
            int numSplits = in.readInt();
            for (int i = 0; i < numSplits; i++) {
                int splitSize = in.readInt();
                byte[] serializedSplit = new byte[splitSize];
                in.read(serializedSplit);
                HBaseSourceSplit split =
                        splitSerializer.deserialize(splitSerializer.getVersion(), serializedSplit);
                checkPoint.add(split);
            }
        }
        return checkPoint;
    }
}
