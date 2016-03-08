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

import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.*;


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

    private Set<String> extractTerms(Aggregation aggregation) {
        Set<String> terms = new HashSet<>();
        if (aggregation instanceof MultiBucketsAggregation) {
            for (MultiBucketsAggregation.Bucket bucket : ((MultiBucketsAggregation) (aggregation)).getBuckets()) {
                if (bucket.getAggregations().asList().size() != 0) {
                    for (Aggregation agg : bucket.getAggregations().asList()) {
                        terms.addAll(extractTerms(agg));
                    }
                } else {
                    terms.add(bucket.getKeyAsString());
                }
            }
        } else {
            throw new IllegalStateException("cannot deal with non bucket aggs");
        }
        return terms;
    }

    @Override
    public void process(final TransportPrepareSpecAction.FieldSpecActionListener fieldSpecActionListener, Client client) {
        client.prepareSearch(this.index).setSource(this.searchRequest).execute(new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                Aggregations agg = searchResponse.getAggregations();
                assert (agg.asList().size() == 1);
                Aggregation termsAgg = agg.asList().get(0);
                Set<String> terms = extractTerms(termsAgg);
                List<String> termsAsList = Lists.newArrayList(terms.toArray(new String[terms.size()]));
                Collections.sort(termsAsList);
                fieldSpecActionListener.onResponse(new StringFieldSpec(termsAsList.toArray(new String[termsAsList.size()]), number, field));
            }

            @Override
            public void onFailure(Throwable throwable) {
                fieldSpecActionListener.onFailure(throwable);
            }
        });
    }
}
