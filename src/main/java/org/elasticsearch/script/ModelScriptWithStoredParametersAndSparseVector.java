package org.elasticsearch.script;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.elasticsearch.common.collect.Tuple;



import java.io.IOException;
import java.util.*;

/**
 * Script for predicting class with a Naive Bayes model
 */
public class ModelScriptWithStoredParametersAndSparseVector extends AbstractSearchScript {

    ClassificationModel model = null;
    String field = null;
    ArrayList features = null;
    Map<String, Integer> wordMap;
    List<Integer> indices = new ArrayList<Integer>();
    List<Integer> values = new ArrayList<Integer>();

    @Override
    public Object run() {
        /** here be the vectorizer **/
        try {
            Fields fields = indexLookup().termVectors();
            if (fields == null) {
                return -1;
            } else {
                Tuple<int[], double[]> indicesAndValues = getIndicesAndValuesSortedByIndex(fields, field, wordMap);
                /** until here **/
                return model.predict(Vectors.sparse(features.size(), indicesAndValues.v1(), indicesAndValues.v2()));
            }
        } catch (IOException ex) {
            throw new ScriptException("Model prediction failed: ", ex);
        }
    }

    Tuple<int[], double[]> getIndicesAndValuesSortedByIndex(Fields fields, String field, Map<String, Integer> wordMap) throws IOException {
        Terms terms = fields.terms(field);
        TermsEnum termsEnum = terms.iterator(null);
        BytesRef t;
        DocsEnum docsEnum = null;

        int counter = 0;
        int numTerms = 0;
        indices.clear();
        while ((t = termsEnum.next()) != null) {
            Integer termIndex  = wordMap.get(t.utf8ToString());
            if (termIndex != null) {
                indices.add(termIndex);
                docsEnum = termsEnum.docs(null, docsEnum);
                int nextDoc = docsEnum.nextDoc();
                assert nextDoc != DocsEnum.NO_MORE_DOCS;
                values.add(docsEnum.freq());
                nextDoc = docsEnum.nextDoc();
                assert nextDoc == DocsEnum.NO_MORE_DOCS;
                numTerms++;
            }
            counter++;
        }
        int[] indicesArray = new int[numTerms];
        double[] valuesArray = new double[numTerms];
        for (int i = 0; i< numTerms ; i++) {
            indicesArray[i] = indices.get(i);
            valuesArray[i] = values.get(i);
        }
        return new Tuple<>(indicesArray, valuesArray);
    }
}
