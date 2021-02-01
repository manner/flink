package org.apache.flink.connector.hbase.sink.committer;

import org.apache.flink.connector.hbase.sink.HBaseSinkCommittable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** HBaseCommittableSerializer. */
public class HBaseCommittableSerializer implements SimpleVersionedSerializer<HBaseSinkCommittable> {
    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(HBaseSinkCommittable committable) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(committable);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public HBaseSinkCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                ObjectInput in = new ObjectInputStream(bais)) {
            HBaseSinkCommittable committable = (HBaseSinkCommittable) in.readObject();
            return committable;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        //        Put put = new Put(Bytes.toBytes("row"));
        //        put.addColumn(
        //                Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("Hallo"));
        return null;
    }
}
