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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.allterms.AllTermsRequestBuilder;
import org.elasticsearch.action.allterms.AllTermsResponse;
import org.elasticsearch.client.Client;

public class StringFieldAllTermsSpecRequest implements FieldSpecRequest {

    private long min_doc_freq;
    private String field;
    String index;
    String number;

    public StringFieldAllTermsSpecRequest(long min_doc_freq, String index, String number, String field) {
        this.min_doc_freq = min_doc_freq;
        this.index = index;
        this.number = number;
        this.field = field;
    }

    @Override
    public void process(final TransportPrepareSpecAction.FieldSpecActionListener fieldSpecActionListener, Client client) {
        new AllTermsRequestBuilder(client).field(field).minDocFreq(min_doc_freq).index(index).size(Integer.MAX_VALUE).execute(new ActionListener<AllTermsResponse>() {
            @Override
            public void onResponse(AllTermsResponse allTerms) {
                fieldSpecActionListener.onResponse(new StringFieldSpec(allTerms.getAllTerms().toArray(new String[allTerms.getAllTerms().size()]), number, field));
            }

            @Override
            public void onFailure(Throwable throwable) {
                fieldSpecActionListener.onFailure(throwable);
            }
        });
    }
}
