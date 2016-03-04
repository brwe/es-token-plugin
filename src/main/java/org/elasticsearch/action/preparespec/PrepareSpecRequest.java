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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class PrepareSpecRequest extends ActionRequest<PrepareSpecRequest> {

    private String source;
    private String index;
    private String type;
    public PrepareSpecRequest() {

    }
    public PrepareSpecRequest(String source, String index, String type) {
        this.source = source;
        this.index = index;
        this.type = type;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (source == null) {
            validationException = ValidateActions.addValidationError("prepare_spec needs a source", validationException);
        }
        if (index == null) {
            validationException = ValidateActions.addValidationError("prepare_spec needs an index", validationException);
        }
        if (type == null) {
            validationException = ValidateActions.addValidationError("prepare_spec needs a type", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        type = in.readString();
        source = in.readString();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeString(type);
        out.writeString(source);
    }

    public PrepareSpecRequest index(String index) {
        this.index = index;
        return this;
    }

    public String index() {
        return index;
    }

    public PrepareSpecRequest type(String type) {
        this.type = type;
        return this;
    }

    public String type( ) {
        return type;
    }

    public PrepareSpecRequest source(String source) {
        this.source = source;
        return this;
    }

    public String source() {
        return source;
    }
}
