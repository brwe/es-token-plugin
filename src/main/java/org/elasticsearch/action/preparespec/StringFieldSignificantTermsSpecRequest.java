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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.ArrayList;
import java.util.List;


public class StringFieldSignificantTermsSpecRequest implements FieldSpecRequest {

    String searchRequest;
    String index;
    String number;
    private String field;

    public StringFieldSignificantTermsSpecRequest(String searchRequest, String index, String number, String field) {
        this.searchRequest = searchRequest;
        this.index = index;
        this.number = number;
        this.field = field;
    }

    private String[] extractTerms(Aggregation aggregation) {
        List<String[]> terms = new ArrayList<>();
        if (aggregation instanceof MultiBucketsAggregation) {
            for (MultiBucketsAggregation.Bucket bucket : ((MultiBucketsAggregation) (aggregation)).getBuckets()) {
                if (bucket.getAggregations().asList().size() != 0) {
                    for (Aggregation agg : bucket.getAggregations().asList()) {
                        terms.add(extractTerms(agg));
                    }
                } else {
                    terms.add(new String[]{bucket.getKeyAsString()});
                }
            }
        } else {
            throw new IllegalStateException("cannot deal with non bucket aggs");
        }
        return combineStringArrays(terms);
    }

    private String[] combineStringArrays(List<String[]> terms) {
        int size = 0;
        for (String[] termsArray : terms) {
            size += termsArray.length;
        }
        String[] finalTerms = new String[size];
        int curIndex = 0;
        for (String[] termsArray : terms) {
            for (String term : termsArray) {
                finalTerms[curIndex] = term;
                curIndex++;
            }
        }
        return finalTerms;
    }

    @Override
    public void process(final TransportPrepareSpecAction.FieldSpecActionListener fieldSpecActionListener, Client client) {
        client.prepareSearch(this.index).setSource(this.searchRequest).execute(new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                Aggregations agg = searchResponse.getAggregations();
                assert (agg.asList().size() == 1);
                Aggregation termsAgg = agg.asList().get(0);
                String[] terms = extractTerms(termsAgg);
                fieldSpecActionListener.onResponse(new StringFieldSpec(terms, number, field));
            }

            @Override
            public void onFailure(Throwable throwable) {
                fieldSpecActionListener.onFailure(throwable);
            }
        });
    }

}
