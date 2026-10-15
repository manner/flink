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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.functions.scalar.ArgumentPreparer;

/** Reads the document once for {@code PARSE_XML} and {@code TRY_PARSE_XML}. */
@Internal
public final class XmlDocumentReader implements ArgumentPreparer<StringData, XmlDocument> {

    private final XmlToVariantParser parser = new XmlToVariantParser();

    @Override
    public XmlDocument prepare(StringData xml) {
        return XmlDocument.read(parser, xml);
    }
}
