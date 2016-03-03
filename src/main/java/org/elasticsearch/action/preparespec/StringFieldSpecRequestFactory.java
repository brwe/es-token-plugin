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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class StringFieldSpecRequestFactory {

    public static FieldSpecRequest createStringFieldSpecRequest(Map<String, Object> parameters, String field) {
        assert field != null;
        if (TokenGenerateMethod.fromString((String) parameters.get("tokens")).equals(TokenGenerateMethod.SIGNIFICANT_TERMS)) {
            parameters.remove("tokens");
            String searchRequest = (String) parameters.remove("request");
            String index = (String) parameters.remove("index");
            String number = (String) parameters.remove("number");
            assertParametersEmpty(parameters);
            return new StringFieldSignificantTermsSpecRequest(searchRequest, index, number, field);
        }
        if (TokenGenerateMethod.fromString((String) parameters.get("tokens")).equals(TokenGenerateMethod.ALL_TERMS)) {
            parameters.remove("tokens");
            String index = (String) parameters.remove("index");
            String number = (String) parameters.remove("number");
            long min_doc_freq = ((Number) parameters.remove("min_doc_freq")).longValue();
            assertParametersEmpty(parameters);
            return new StringFieldAllTermsSpecRequest(min_doc_freq, index, number, field);
        }
        if (TokenGenerateMethod.fromString((String) parameters.get("tokens")).equals(TokenGenerateMethod.GIVEN)) {
            parameters.remove("tokens");
            String number = (String) parameters.remove("number");
            ArrayList<String> terms = (ArrayList<String>) parameters.remove("terms");
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
