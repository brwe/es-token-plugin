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

import org.elasticsearch.ElasticsearchException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class StringFieldSpecRequestFactory {

    public static FieldSpecRequest createStringFieldSpecRequest(Map<String, Object> parameters) {
        String field = (String) parameters.remove("field");
        if (field == null) {
            throw new ElasticsearchException("field parameter missing from prepare spec request");
        }
        String tokens = (String) parameters.remove("tokens");
        if (field == null) {
            throw new ElasticsearchException("tokens parameter missing from prepare spec request");
        }
        String number = (String) parameters.remove("number");
        if (number == null) {
            throw new ElasticsearchException("number parameter missing from prepare spec request");
        }
        if (TokenGenerateMethod.fromString(tokens).equals(TokenGenerateMethod.SIGNIFICANT_TERMS)) {
            String searchRequest = (String) parameters.remove("request");
            if (searchRequest == null) {
                throw new ElasticsearchException("request parameter missing from prepare spec request");
            }
            String index = (String) parameters.remove("index");
            if (index == null) {
                throw new ElasticsearchException("index parameter missing from prepare spec request");
            }
            assertParametersEmpty(parameters);
            return new StringFieldSignificantTermsSpecRequest(searchRequest, index, number, field);
        }
        if (TokenGenerateMethod.fromString(tokens).equals(TokenGenerateMethod.ALL_TERMS)) {
            String index = (String) parameters.remove("index");
            if (index == null) {
                throw new ElasticsearchException("index parameter missing from prepare spec request");
            }
            Object min_doc_freq_obj = parameters.remove("min_doc_freq");
            if (min_doc_freq_obj == null) {
                throw new ElasticsearchException("min_doc_freq parameter missing from prepare spec request");
            }
            long min_doc_freq = ((Number) min_doc_freq_obj).longValue();
            assertParametersEmpty(parameters);
            return new StringFieldAllTermsSpecRequest(min_doc_freq, index, number, field);
        }
        if (TokenGenerateMethod.fromString(tokens).equals(TokenGenerateMethod.GIVEN)) {
            ArrayList<String> terms = (ArrayList<String>) parameters.remove("terms");
            if (terms == null) {
                throw new ElasticsearchException("terms parameter missing from prepare spec request");
            }
            assertParametersEmpty(parameters);
            return new StringFieldGivenTermsSpecRequest(terms.toArray(new String[terms.size()]), number, field);
        }
        throw new UnsupportedOperationException("Have not implemented given yet!");
    }

    private static void assertParametersEmpty(Map<String, Object> parameters) {
        if (parameters.isEmpty() == false) {
            throw new IllegalStateException("found additional parameters and don't know what to do with them!" + Arrays.toString(parameters.keySet().toArray(new String[parameters.size()])));
        }
    }
}
