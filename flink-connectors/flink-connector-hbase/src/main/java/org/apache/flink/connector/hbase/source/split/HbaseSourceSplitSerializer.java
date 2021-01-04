package org.apache.flink.connector.hbase.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer serializer} for {@link HbaseSourceSplit}.
 */
public class HbaseSourceSplitSerializer implements SimpleVersionedSerializer<HbaseSourceSplit> {
	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(HbaseSourceSplit obj) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream out = new DataOutputStream(baos)) {
			out.writeUTF("test");
			out.flush();
			return baos.toByteArray();
		}
	}

	@Override
	public HbaseSourceSplit deserialize(int version, byte[] serialized) throws IOException {
		return new HbaseSourceSplit("2");
	}
}
