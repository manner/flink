package org.apache.flink.connector.hbase.sink;

import org.apache.hadoop.hbase.client.Mutation;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/** HBaseCommittable. */
public class HBaseSinkCommittable implements Serializable {
    private List<Mutation> mutations;

    public HBaseSinkCommittable(Mutation mutation) {
        this.mutations = Collections.singletonList(mutation);
    }

    public List<Mutation> getMutations() {
        return mutations;
    }
}
