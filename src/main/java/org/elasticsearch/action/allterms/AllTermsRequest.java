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

package org.elasticsearch.action.allterms;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AllTermsRequest extends ActionRequest<AllTermsRequest> {

    String preference;
    private String field;
    private String index;
    private int size;
    private String from;
    private long minDocFreq;

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (field == null) {
            validationException = ValidateActions.addValidationError("all terms request need a field name", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        preference = in.readOptionalString();
        field = in.readString();
        index = in.readString();
        size = in.readInt();
        from = in.readOptionalString();
        minDocFreq = in.readLong();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(preference);
        out.writeString(field);
        out.writeString(index);
        out.writeInt(size);
        out.writeOptionalString(from);
        out.writeLong(minDocFreq);
    }

    public void field(String field) {
        this.field = field;
    }

    public void index(String index) {
        this.index = index;
    }

    public void size(int size) {
        this.size = size;
    }

    public void minDocFreq(long minDocFreq) {
        this.minDocFreq = minDocFreq;
    }

    public int size() {
        return size;
    }

    public String[] indices() {
        String[] indices = {index};
        return indices;
    }

    public String field() {
        return field;
    }

    public void from(String from) {
        this.from = from;
    }

    public String from() {
        return from;
    }

    public long minDocFreq() {
        return minDocFreq;
    }
}
