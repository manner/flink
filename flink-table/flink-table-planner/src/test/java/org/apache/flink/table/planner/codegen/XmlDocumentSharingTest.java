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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.functions.XmlDocument;
import org.apache.flink.table.types.DataType;

import java.util.List;

/** Checks that {@code PARSE_XML} and {@code TRY_PARSE_XML} share the read document. */
class XmlDocumentSharingTest extends SharedArgumentFunctionTestBase {

    @Override
    protected Class<?> valueClass() {
        return XmlDocument.class;
    }

    @Override
    protected List<String> calls() {
        return List.of(
                "PARSE_XML(%s)",
                "PARSE_XML(%s, TRUE)", "TRY_PARSE_XML(%s)", "TRY_PARSE_XML(%s, TRUE)");
    }

    @Override
    protected DataType inputType() {
        return DataTypes.STRING();
    }

    @Override
    protected Object input() {
        return "<book><title>Dune</title></book>";
    }

    @Override
    protected Object otherInput() {
        return "<a>1</a>";
    }
}
