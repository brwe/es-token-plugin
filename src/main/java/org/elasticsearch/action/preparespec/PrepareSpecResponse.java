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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public class PrepareSpecResponse extends ActionResponse implements ToXContent {

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public int getLength() {
        return length;
    }

    String index;
    String type;
    String id;
    int length;

    public PrepareSpecResponse() {

    }

    public PrepareSpecResponse(String index, String type, String id, int length) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.length = length;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.INDEX, index);
        builder.field(Fields.TYPE, type);
        builder.field(Fields.ID, id);
        builder.field(Fields.LENGTH, length);
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString INDEX = new XContentBuilderString("index");
        static final XContentBuilderString TYPE = new XContentBuilderString("type");
        static final XContentBuilderString ID = new XContentBuilderString("id");
        static final XContentBuilderString LENGTH = new XContentBuilderString("length");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        type = in.readString();
        id = in.readString();
        length = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeString(type);
        out.writeString(id);
        out.writeInt(length);
    }
}