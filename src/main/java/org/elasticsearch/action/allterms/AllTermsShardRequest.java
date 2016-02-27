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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AllTermsShardRequest extends SingleShardRequest<AllTermsShardRequest> {

    private String from;
    private int shardId;
    private String preference;

    private String field;

    int size = 0;
    private long minDocFreq = 0;

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    AllTermsShardRequest(AllTermsRequest request, String index, int shardId, String field, int size, String from, long minDocFreq) {
        super(request, index);
        this.shardId = shardId;
        this.field = field;
        this.size = size;
        this.from = from;
        this.minDocFreq = minDocFreq;
    }

    public AllTermsShardRequest() {

    }

    public int shardId() {
        return this.shardId;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public AllTermsShardRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        preference = in.readOptionalString();
        field = in.readString();
        size = in.readInt();
        from = in.readOptionalString();
        minDocFreq = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(preference);
        out.writeString(field);
        out.writeInt(size);
        out.writeOptionalString(from);
        out.writeLong(minDocFreq);
    }

    public String field() {
        return field;
    }

    public long size() {
        return size;
    }

    public String from() {
        return from;
    }

    public long minDocFreq() {
        return minDocFreq;
    }
}
