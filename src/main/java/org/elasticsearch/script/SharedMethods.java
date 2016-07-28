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

package org.elasticsearch.script;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafIndexLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class SharedMethods {
    public static Tuple<int[], double[]> getIndicesAndValuesFromTermVectors(Fields fields, String field, Map<String, Integer> wordMap)
            throws IOException {
        List<Integer> indices = new ArrayList<>();
        List<Double> values = new ArrayList<>();
        Terms terms = fields.terms(field);
        TermsEnum termsEnum = terms.iterator();
        BytesRef t;
        PostingsEnum docsEnum = null;

        int numTerms = 0;
        indices.clear();
        values.clear();
        while ((t = termsEnum.next()) != null) {
            Integer termIndex = wordMap.get(t.utf8ToString());
            if (termIndex != null) {
                indices.add(termIndex);
                docsEnum = termsEnum.postings(docsEnum, PostingsEnum.FREQS);
                int nextDoc = docsEnum.nextDoc();
                assert nextDoc != PostingsEnum.NO_MORE_DOCS;
                values.add((double) docsEnum.freq());
                nextDoc = docsEnum.nextDoc();
                assert nextDoc == PostingsEnum.NO_MORE_DOCS;
                numTerms++;
            }
        }
        int[] indicesArray = new int[numTerms];
        double[] valuesArray = new double[numTerms];
        for (int i = 0; i < numTerms; i++) {
            indicesArray[i] = indices.get(i);
            valuesArray[i] = values.get(i);
        }
        return new Tuple<>(indicesArray, valuesArray);
    }

    static public Tuple<int[], double[]> getIndicesAndValuesFromFielddataFields(Map<String, Integer> wordMap, ScriptDocValues<String>
            docValues) {
        Tuple<int[], double[]> indicesAndValues;
        List<Integer> indices = new ArrayList<>();

        for (String value : docValues.getValues()) {
            Integer index = wordMap.get(value);
            if (index != null) {
                indices.add(index);
            }
        }
        int[] indicesArray = new int[indices.size()];
        double[] valuesArray = new double[indices.size()];
        for (int i = 0; i < indices.size(); i++) {
            indicesArray[i] = indices.get(i);
            valuesArray[i] = 1;
        }
        indicesAndValues = new Tuple<>(indicesArray, valuesArray);
        return indicesAndValues;
    }

    public static Map<String, Object> getSourceAsMap(String source) throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(source);
        return parser.mapOrdered();
    }

    public static Tuple<int[], double[]> getIndicesAndTF_IDFFromTermVectors(Fields fields, String field, Map<String, Integer> wordMap,
                                                                            LeafIndexLookup indexLookup) throws IOException {
        List<Integer> indices = new ArrayList<>();
        List<Double> values = new ArrayList<>();
        Terms terms = fields.terms(field);
        TermsEnum termsEnum = terms.iterator();
        BytesRef t;
        PostingsEnum docsEnum = null;
        double docCount = indexLookup.getParentReader().numDocs();

        int numTerms = 0;
        indices.clear();
        values.clear();
        while ((t = termsEnum.next()) != null) {
            Integer termIndex = wordMap.get(t.utf8ToString());
            if (termIndex != null) {
                indices.add(termIndex);
                docsEnum = termsEnum.postings(docsEnum, PostingsEnum.FREQS);
                int nextDoc = docsEnum.nextDoc();
                assert nextDoc != PostingsEnum.NO_MORE_DOCS;
                double docFreq = indexLookup.getParentReader().docFreq(new Term(field, t));
                double freq = docsEnum.freq();
                values.add(freq * Math.log((docCount + 1) / (docFreq + 1)));
                nextDoc = docsEnum.nextDoc();
                assert nextDoc == PostingsEnum.NO_MORE_DOCS;
                numTerms++;
            }
        }
        int[] indicesArray = new int[numTerms];
        double[] valuesArray = new double[numTerms];
        for (int i = 0; i < numTerms; i++) {
            indicesArray[i] = indices.get(i);
            valuesArray[i] = values.get(i);
        }
        return new Tuple<>(indicesArray, valuesArray);
    }
}
