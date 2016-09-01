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

package org.elasticsearch.ml.training;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.AbstractObjectParser;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Defines a subset of data to perform training and verification on
 */
public class DataSet implements Writeable {

    private final String index;

    private final String type;

    private final Map<String, Object> query;

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<DataSet, ParseFieldMatcherSupplier> PARSER =
            new ConstructingObjectParser<>("data_set", a -> new DataSet((String) a[0], (String) a[1], (Map<String, Object>) a[2]));

    static {
        PARSER.declareString(constructorArg(), new ParseField("index"));
        PARSER.declareString(constructorArg(), new ParseField("type"));
        PARSER.declareField(optionalConstructorArg(), XContentParser::mapOrdered, new ParseField("query"), ObjectParser.ValueType.OBJECT);
    }

    public DataSet(String index, String type) {
        this(index, type, null);
    }

    public DataSet(String index, String type, Map<String, Object> query) {
        this.index = index;
        this.type = type;
        this.query = query == null? Collections.emptyMap() : query;
    }

    public DataSet(StreamInput input) throws IOException {
        index = input.readString();
        type = input.readString();
        query = input.readMap();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(type);
        out.writeMap(query);
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSet dataSet = (DataSet) o;
        return Objects.equals(index, dataSet.index) &&
                Objects.equals(type, dataSet.type) &&
                Objects.equals(query, dataSet.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, query);
    }

    @Override
    public String toString() {
        return "DataSet{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                ", query=" + query +
                '}';
    }
}

