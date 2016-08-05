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

package org.elasticsearch.script.modelinput;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafIndexLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch Data Source
 */
public abstract class EsDataSource implements DataSource {

    protected abstract LeafDocLookup getDocLookup();

    protected abstract LeafIndexLookup getLeafIndexLookup();

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> getValues(String field) {
        return (List<T>) getDocLookup().get(field);
    }

    @Override
    public double[] getOccurrenceDense(String[] terms, String field) {
        return getDense(terms, field, (indexField, indexFieldTerm) -> indexFieldTerm.tf() > 0 ? 1 : 0);
    }

    @Override
    public double[] getTfIdfDense(String[] terms, String field) {
        return getDense(terms, field, (indexField, indexFieldTerm) -> {
            double tf = indexFieldTerm.tf();
            double df = indexFieldTerm.df();
            double numDocs = indexField.docCount();
            return tf * Math.log((numDocs + 1) / (df + 1));
        });
    }

    @Override
    public double[] getTfDense(String[] terms, String field) {
        return getDense(terms, field, (indexField, indexFieldTerm) -> indexFieldTerm.tf());
    }

    @Override
    public Tuple<int[], double[]> getTfSparse(Map<String, Integer> wordMap, String field) {
        return getSparse(wordMap, field, (docsEnum, term) -> (double) docsEnum.freq());
    }

    @Override
    public Tuple<int[], double[]> getTfIdfSparse(Map<String, Integer> wordMap, String field) {
        return getSparse(wordMap, field, (docsEnum, term) -> {
            double docFreq = getLeafIndexLookup().getParentReader().docFreq(new Term(field, term));
            double freq = docsEnum.freq();
            double docCount = getLeafIndexLookup().getParentReader().numDocs();
            return freq * Math.log((docCount + 1) / (docFreq + 1));
        });
    }

    private interface IndexFieldTermFunction {
        double apply(IndexField indexField, IndexFieldTerm indexFieldTerm) throws IOException;
    }

    private double[] getDense(String[] terms, String field, IndexFieldTermFunction f) {
        double[] values = new double[terms.length];
        IndexField indexField = getLeafIndexLookup().get(field);
        for (int i = 0; i < terms.length; i++) {
            IndexFieldTerm indexTermField = indexField.get(terms[i]);
            try {
                values[i] = f.apply(indexField, indexTermField);
            } catch (IOException ex) {
                throw new IllegalArgumentException("cannot get dense vector for field " + field + " for term "+ terms[i], ex);
            }

        }
        return values;
    }

    private interface DocsEnumFunction {
        double apply(PostingsEnum docsEnum, BytesRef term) throws IOException;
    }

    private Tuple<int[], double[]> getSparse(Map<String, Integer> wordMap, String field, DocsEnumFunction function) {
        try {
            Fields fields = getLeafIndexLookup().termVectors();
            if (fields == null) {
                return null;
            } else {
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
                        values.add(function.apply(docsEnum, t));
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
        } catch (IOException ex) {
            throw new IllegalArgumentException("cannot get sparse tf/idf vector for field "+ field, ex);
        }
    }
}
