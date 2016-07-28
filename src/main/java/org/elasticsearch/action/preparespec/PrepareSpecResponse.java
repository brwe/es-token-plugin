/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.preparespec;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class PrepareSpecResponse extends ActionResponse implements ToXContent {

    private BytesReference spec;
    private Map<String, Object> specAsMap;
    private int length;

    public PrepareSpecResponse() {

    }

    public PrepareSpecResponse(BytesReference spec, int length) {
        this.spec = spec;
        this.length = length;
    }
    public BytesReference getSpec() {
        return spec;
    }

    public Map<String, Object> getSpecAsMap() {
        if (specAsMap == null) {
            specAsMap = Collections.unmodifiableMap(XContentHelper.convertToMap(spec, true).v2());
        }
        return specAsMap;
    }

    public int getLength() {
        return length;
    }


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.rawField(Fields.SPEC, spec);
        builder.field(Fields.LENGTH, length);
        return builder;
    }

    static final class Fields {
        static final String SPEC = "spec";
        static final String LENGTH = "length";
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        spec = in.readBytesReference();
        length = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(spec);
        out.writeInt(length);
    }
}