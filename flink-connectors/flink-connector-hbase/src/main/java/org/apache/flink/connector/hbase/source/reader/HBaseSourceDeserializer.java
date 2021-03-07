package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.Serializable;

/** Deserialization Interface. */
public abstract class HBaseSourceDeserializer<T> implements Serializable, ResultTypeQueryable<T> {
    private static final long serialVersionUID = 1L;
    private final TypeInformation<T> type;

    protected HBaseSourceDeserializer() {
        try {
            this.type =
                    TypeExtractor.createTypeInfo(
                            HBaseSourceDeserializer.class, getClass(), 0, null, null);
        } catch (InvalidTypesException e) {
            throw new FlinkRuntimeException(
                    "The implementation of AbstractDeserializationSchema is using a generic variable. "
                            + "This is not supported, because due to Java's generic type erasure, it will not be possible to "
                            + "determine the full type at runtime. For generic implementations, please pass the TypeInformation "
                            + "or type class explicitly to the constructor.");
        }
    }

    public abstract T deserialize(HBaseEvent event);

    @Override
    public TypeInformation<T> getProducedType() {
        return type;
    }
}
