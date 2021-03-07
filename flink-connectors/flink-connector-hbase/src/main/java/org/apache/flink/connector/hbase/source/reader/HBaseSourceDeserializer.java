/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
