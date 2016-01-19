package org.elasticsearch.script;

import org.elasticsearch.common.collect.Tuple;


public interface EsModelEvaluator {
    String evaluate(Tuple<int[], double[]> featureValues);
}
