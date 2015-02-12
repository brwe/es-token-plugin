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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AllTermsResponse extends ActionResponse implements ToXContent {

    List<String> allTerms = new ArrayList<>();

    public AllTermsResponse() {

    }

    public AllTermsResponse(AllTermsSingleShardResponse[] responses, long size) {
        int numResponses = responses.length;
        int[] indices = new int[numResponses];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = 0;
        }

        String lastTerm = null;

        //find the first twerm
        for (int j = 0; j < numResponses; j++) {
            if (responses[j] != null && responses[j].shardTerms != null) {
                if (indices[j] < responses[j].shardTerms.size()) {
                    if (lastTerm == null) {
                        lastTerm = responses[j].shardTerms.get(indices[j]);
                        indices[j]++;
                    } else {
                        if (responses[j].shardTerms.get(indices[j]).compareTo(lastTerm) == 0) {
                            //move one further
                            indices[j]++;
                        }
                        if (indices[j] < responses[j].shardTerms.size()) {
                            String candidate = responses[j].shardTerms.get(indices[j]);
                            if (candidate == null) {
                                continue;
                            }

                            if (candidate.compareTo(lastTerm) < 0) {
                                lastTerm = candidate;
                            }
                        }

                    }
                }
            }
        }
        if (lastTerm != null) {
            allTerms.add(lastTerm);
        }
        for (int i = 0; i < size; i++) {

            String curTerm = null;
            for (int j = 0; j < numResponses; j++) {
                if (responses[j] != null && responses[j].shardTerms != null) {
                    if (indices[j] < responses[j].shardTerms.size()) {
                        if (lastTerm == null) {
                            curTerm = responses[j].shardTerms.get(indices[j]);
                            indices[j]++;
                        } else {
                            if (responses[j].shardTerms.get(indices[j]).compareTo(lastTerm) == 0) {
                                //move one further
                                indices[j]++;
                                if (indices[j] >= responses[j].shardTerms.size()) {
                                    continue;
                                }
                            }
                            String candidate = responses[j].shardTerms.get(indices[j]);
                            if (candidate == null) {
                                continue;
                            }
                            if (curTerm == null) {
                                curTerm = candidate;
                            } else {
                                if (candidate.compareTo(curTerm) < 0) {
                                    curTerm = candidate;
                                }
                            }
                        }
                    }
                }
            }
            if (curTerm != null) {
                allTerms.add(curTerm);
                lastTerm = curTerm;
            } else {
                break;
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TERMS, allTerms);
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString TERMS = new XContentBuilderString("terms");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        allTerms = Arrays.asList(in.readStringArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(allTerms.toArray(new String[allTerms.size()]));
    }
}