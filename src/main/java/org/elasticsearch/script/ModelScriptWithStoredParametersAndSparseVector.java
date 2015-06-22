package org.elasticsearch.script;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;

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

    Comparator<IndexAndValue> comparator = new Comparator<IndexAndValue>() {
        public int compare(IndexAndValue a,
                           IndexAndValue b) {
            return a.index > b.index ? 1 : (a.index == b.index ? 0 : -1);
        }
    };

    @Override
    public Object run() {
        /** here be the vectorizer **/
        List<IndexAndValue> indicesAndValues = new ArrayList<>();
        try {
            Fields fields = indexLookup().termVectors();
            Terms terms = fields.terms(field);
            TermsEnum termsEnum = terms.iterator(null);
            BytesRef t;
            DocsEnum docsEnum = null;
            while ((t = termsEnum.next()) != null) {
                Integer ind = wordMap.get(t.utf8ToString());
                if (ind != null) {
                    docsEnum = termsEnum.docs(null, docsEnum);
                    int nextDoc = docsEnum.nextDoc();
                    assert nextDoc != DocsEnum.NO_MORE_DOCS;
                    int freq = docsEnum.freq();
                    nextDoc = docsEnum.nextDoc();
                    assert nextDoc == DocsEnum.NO_MORE_DOCS;
                    indicesAndValues.add(new IndexAndValue(ind.intValue(), freq));
                }
            }
            Collections.sort(indicesAndValues, comparator);

            int[] indices = new int[indicesAndValues.size()];
            double[] values = new double[indicesAndValues.size()];
            for (int i = 0; i < indicesAndValues.size(); i++) {
                indices[i] = indicesAndValues.get(i).index;
                values[i] = indicesAndValues.get(i).value;
            }
            /** until here **/
            return model.predict(Vectors.sparse(features.size(), indices, values));
        } catch (IOException ex) {
            throw new ScriptException("Model prediction failed: ", ex);
        }
    }

    public static class IndexAndValue {
        int index;
        double value;

        public IndexAndValue(int index, double value) {
            this.index = index;
            this.value = value;
        }
    }
}
