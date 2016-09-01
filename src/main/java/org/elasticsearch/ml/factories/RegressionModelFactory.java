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

package org.elasticsearch.ml.factories;

import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.NumericPredictor;
import org.dmg.pmml.OpType;
import org.dmg.pmml.RegressionModel;
import org.dmg.pmml.RegressionTable;
import org.dmg.pmml.TransformationDictionary;
import org.elasticsearch.ml.modelinput.ModelAndModelInputEvaluator;
import org.elasticsearch.ml.modelinput.PMMLVectorRange;
import org.elasticsearch.ml.modelinput.VectorModelInput;
import org.elasticsearch.ml.modelinput.VectorModelInputEvaluator;
import org.elasticsearch.ml.modelinput.VectorRange;
import org.elasticsearch.ml.models.EsLinearSVMModel;
import org.elasticsearch.ml.models.EsLogisticRegressionModel;
import org.elasticsearch.ml.models.EsModelEvaluator;
import org.elasticsearch.script.pmml.ProcessPMMLHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for regression models
 */
public class RegressionModelFactory extends ModelFactory<VectorModelInput, String, RegressionModel> {

    public RegressionModelFactory() {
        super(RegressionModel.class);
    }


    @Override
    public ModelAndModelInputEvaluator<VectorModelInput, String> buildFromPMML(RegressionModel model, DataDictionary dataDictionary,
                                                                               TransformationDictionary transformationDictionary) {
        if (model.getModelName().equals("logistic regression")) {
            return initModel(model, dataDictionary, transformationDictionary, EsLogisticRegressionModel::new);
        } else if (model.getModelName().equals("linear SVM")) {
            return initModel(model, dataDictionary, transformationDictionary, EsLinearSVMModel::new);
        } else {
            throw new UnsupportedOperationException("We only implemented logistic regression so far but your model is of type " +
                    model.getModelName());
        }
    }

    private interface RegressionModelConstructor {
        EsModelEvaluator<VectorModelInput, String> create(double[] coefficients, double intercept, String[] classes);

    }

    private ModelAndModelInputEvaluator<VectorModelInput, String> initModel(RegressionModel model,
                                                                            DataDictionary dataDictionary,
                                                                            TransformationDictionary transformationDictionary,
                                                                            RegressionModelConstructor constructor) {
        List<VectorRange> vectorRanges = new ArrayList<>();
        int indexCounter = 0;
        Map<String, OpType> types = new HashMap<>();
        // TODO: add
        RegressionTable regressionTable = model.getRegressionTables().get(0);
        for (NumericPredictor predictor : regressionTable.getNumericPredictors()) {
            PMMLVectorRange vectorRange = ProcessPMMLHelper.extractVectorRange(model, dataDictionary,
                    transformationDictionary, predictor.getName().getValue(), () -> {
                        throw new IllegalArgumentException("Categorical fields are not supported yet");
                    }, indexCounter, types);
            vectorRanges.add(vectorRange);
            indexCounter += vectorRange.size();
        }
        VectorModelInputEvaluator vectorPMML = new VectorModelInputEvaluator(vectorRanges);
        EsModelEvaluator<VectorModelInput, String> modelEvaluator = buildLinerModel(model, constructor);
        return new ModelAndModelInputEvaluator<>(vectorPMML, modelEvaluator);
    }

    private static EsModelEvaluator<VectorModelInput, String> buildLinerModel(RegressionModel model,
                                                                              RegressionModelConstructor constructor) {
        RegressionTable regressionTable = model.getRegressionTables().get(0);
        List<NumericPredictor> numericPredictors = regressionTable.getNumericPredictors();
        double[] coefficients = new double[numericPredictors.size()];
        int i = 0;
        for (NumericPredictor numericPredictor : numericPredictors) {
            coefficients[i] = numericPredictor.getCoefficient();
            i++;
        }
        String[] classes = new String[]{
                model.getRegressionTables().get(0).getTargetCategory(),
                model.getRegressionTables().get(1).getTargetCategory()
        };
        return constructor.create(coefficients, regressionTable.getIntercept(), classes);
    }

}
