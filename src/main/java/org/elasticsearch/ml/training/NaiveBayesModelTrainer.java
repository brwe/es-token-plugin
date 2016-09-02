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

package org.elasticsearch.ml.training;

import org.dmg.pmml.BayesInput;
import org.dmg.pmml.BayesInputs;
import org.dmg.pmml.BayesOutput;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.GaussianDistribution;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.NaiveBayesModel;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.PairCounts;
import org.dmg.pmml.TargetValueCount;
import org.dmg.pmml.TargetValueCounts;
import org.dmg.pmml.TargetValueStat;
import org.dmg.pmml.TargetValueStats;
import org.dmg.pmml.Value;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.jpmml.model.JAXBUtil;

import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

/**
 *
 */
public class NaiveBayesModelTrainer implements ModelTrainer {
    @Override
    public String modelType() {
        return "naive_bayes";
    }

    @Override
    public TrainingSession createTrainingSession(MappingMetaData mappingMetaData, List<ModelInputField> inputs, ModelTargetField target,
                                                 Settings settings) {
        return new NaiveBayesTrainingSession(mappingMetaData, inputs, target, settings);
    }

    private static class NaiveBayesTrainingSession implements TrainingSession {

        final TermsAggregationBuilder termsAggregationBuilder;

        private NaiveBayesTrainingSession(MappingMetaData mappingMetaData, List<ModelInputField> fields, ModelTargetField target,
                                          Settings settings) {
            TermsAggregationBuilder topLevelClassAgg = terms(target.getName());
            topLevelClassAgg.field(target.getName());
            topLevelClassAgg.size(Integer.MAX_VALUE);
            topLevelClassAgg.shardMinDocCount(1);
            topLevelClassAgg.minDocCount(1);
            topLevelClassAgg.order(Terms.Order.term(true));
            Map<String, Object> fieldMappings = getFiledMappings(mappingMetaData);
            for (ModelInputField field : fields) {
                String fieldType = getFieldType(fieldMappings, field.getName());
                if (fieldType == null) {
                    throw new IllegalArgumentException("input field [" + field.getName() + "] not found");
                }
                if (fieldType.equals("text") || fieldType.equals("keyword")) {
                    topLevelClassAgg.subAggregation(terms(field.getName()).field(field.getName())
                                    .size(Integer.MAX_VALUE).shardMinDocCount(1).minDocCount(1)
                                    .order(Terms.Order.term(true)));
                } else if (fieldType.equals("double") || fieldType.equals("float") || fieldType.equals("integer") ||
                        fieldType.equals("long")) {
                    topLevelClassAgg.subAggregation(extendedStats(field.getName()).field(field.getName()));
                } else {
                    throw new UnsupportedOperationException("have not implemented naive bayes training for anything but " +
                            "number and string field yet");
                }
            }
            termsAggregationBuilder = topLevelClassAgg;
        }


        @SuppressWarnings("unchecked")
        private Map<String, Object> getFiledMappings(MappingMetaData mappingMetaData) {
            try {
                return (Map<String, Object>) mappingMetaData.sourceAsMap().get("properties");
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        }

        @SuppressWarnings("unchecked")
        private String getFieldType(Map<String, Object> fieldMappings, String field) {
            Map<String, Object> attributes = (Map<String, Object>) fieldMappings.get(field);
            return (String) attributes.get("type");
        }

        @Override
        public AggregationBuilder trainingRequest() {
            return termsAggregationBuilder;
        }

        @Override
        public String model(SearchResponse searchResponse) {
            NaiveBayesModel naiveBayesModel = new NaiveBayesModel();
            Aggregations aggs = searchResponse.getAggregations();
            Terms classAgg = (Terms) aggs.asList().get(0);
            int numClasses = classAgg.getBuckets().size();
            long[] classCounts = new long[numClasses];
            String[] classLabels = new String[numClasses];
            int classCounter = 0;

            for (Terms.Bucket bucket : classAgg.getBuckets()) {
                classCounts[classCounter] = bucket.getDocCount();
                classLabels[classCounter] = bucket.getKeyAsString();
                classCounter++;
            }
            if (classCounter < 2) {
                throw new RuntimeException("Need at least two classes for naive bayes!");
            }
            setTargetValueCounts(naiveBayesModel, classAgg, classCounts, classLabels);

            // field, value, class -> count
            TreeMap<String, TreeMap<String, TreeMap<String, Long>>> stringFieldValueCounts = new TreeMap<>();
            TreeMap<String, TreeSet<String>> allTermsPerField = new TreeMap<>();
            TreeMap<String, TreeMap<String, Map<String, Double>>> numericFieldStats = new TreeMap<>();
            for (Terms.Bucket bucket : classAgg.getBuckets()) {
                String className = bucket.getKeyAsString();
                for (Aggregation aggregation : bucket.getAggregations()) {
                    String fieldName = aggregation.getName();
                    if (aggregation instanceof Terms) {
                        Terms termAgg = (Terms) aggregation;
                        // init the data structure if not present
                        if (stringFieldValueCounts.containsKey(fieldName) == false) {
                            stringFieldValueCounts.put(fieldName, new TreeMap<>());
                            allTermsPerField.put(fieldName, new TreeSet<>());
                        }
                        TreeMap<String, TreeMap<String, Long>> valueCounts = stringFieldValueCounts.get(fieldName);


                        for (Terms.Bucket termBucket : termAgg.getBuckets()) {
                            String value = termBucket.getKeyAsString();
                            if (valueCounts.containsKey(value) == false) {
                                valueCounts.put(value, new TreeMap<>());
                            }
                            TreeMap<String, Long> termCountsPerClass = valueCounts.get(value);
                            allTermsPerField.get(fieldName).add(termBucket.getKeyAsString());
                            termCountsPerClass.put(className, termBucket.getDocCount());
                        }
                    } else if (aggregation instanceof ExtendedStats) {
                        ExtendedStats extendedStats = (ExtendedStats) aggregation;
                        if (numericFieldStats.containsKey(fieldName) == false) {
                            numericFieldStats.put(fieldName, new TreeMap<>());
                        }

                        Map<String, Double> stats = new HashMap<>();
                        stats.put("mean", extendedStats.getAvg());
                        stats.put("variance", extendedStats.getVariance());
                        numericFieldStats.get(fieldName).put(className, stats);
                    } else {
                        throw new RuntimeException("unsupported agg " + aggregation.getClass().getName());
                    }
                }
            }
            setBayesInputs(naiveBayesModel, stringFieldValueCounts, numericFieldStats, classLabels);
            naiveBayesModel.setFunctionName(MiningFunctionType.CLASSIFICATION);

            final PMML pmml = new PMML();
            setDataDictionary(pmml, allTermsPerField, numericFieldStats.keySet());
            setMiningFields(naiveBayesModel, allTermsPerField.keySet(), numericFieldStats.keySet(), classAgg.getName());

            naiveBayesModel.setThreshold(1.0 / searchResponse.getHits().totalHits());
            pmml.addModels(naiveBayesModel);
            final StreamResult streamResult = new StreamResult();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            streamResult.setOutputStream(outputStream);

            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        JAXBUtil.marshal(pmml, streamResult);
                    } catch (JAXBException e) {
                        throw new RuntimeException("No idea what went wrong here", e);
                    }
                    return null;
                }
            });
            return new String(outputStream.toByteArray(), Charset.defaultCharset());
        }
    }

    private static void setMiningFields(NaiveBayesModel naiveBayesModel, Set<String> categoricalFields, Set<String> numericFields,
                                        String classField) {
        MiningSchema miningSchema = new MiningSchema();
        for (String fieldName : categoricalFields) {
            MiningField miningField = new MiningField();
            miningField.setName(new FieldName(fieldName));
            miningField.setUsageType(FieldUsageType.ACTIVE);
            miningSchema.addMiningFields(miningField);
        }
        for (String fieldName : numericFields) {
            MiningField miningField = new MiningField();
            miningField.setName(new FieldName(fieldName));
            miningField.setUsageType(FieldUsageType.ACTIVE);
            miningSchema.addMiningFields(miningField);
        }
        MiningField miningField = new MiningField();
        miningField.setName(new FieldName(classField));
        miningField.setUsageType(FieldUsageType.PREDICTED);
        miningSchema.addMiningFields(miningField);
        naiveBayesModel.setMiningSchema(miningSchema);
    }

    private static void setBayesInputs(NaiveBayesModel naiveBayesModel,
                                       TreeMap<String, TreeMap<String, TreeMap<String, Long>>> stringFieldValueCounts,
                                       TreeMap<String, TreeMap<String, Map<String, Double>>> numericFieldStats, String[] classNames) {
        BayesInputs bayesInputs = new BayesInputs();
        for (Map.Entry<String, TreeMap<String, TreeMap<String, Long>>> categoricalField : stringFieldValueCounts.entrySet()) {
            String fieldName = categoricalField.getKey();
            BayesInput bayesInput = new BayesInput();
            bayesInput.setFieldName(new FieldName(fieldName));
            for (Map.Entry<String, TreeMap<String, Long>> valueCounts : categoricalField.getValue().entrySet()) {
                String value = valueCounts.getKey();
                PairCounts pairCounts = new PairCounts();
                pairCounts.setValue(value);
                TargetValueCounts targetValueCounts = new TargetValueCounts();
                TreeMap<String, Long> classCounts = valueCounts.getValue();
                for (String className : classNames) {
                    if (classCounts.containsKey(className)) {
                        targetValueCounts.addTargetValueCounts(new TargetValueCount().setValue(className).setCount(classCounts.get
                                (className)));

                    } else {
                        targetValueCounts.addTargetValueCounts(new TargetValueCount().setValue(className).setCount(0));
                    }
                }
                pairCounts.setTargetValueCounts(targetValueCounts);
                bayesInput.addPairCounts(pairCounts);
            }
            bayesInputs.addBayesInputs(bayesInput);
        }
        for (Map.Entry<String, TreeMap<String, Map<String, Double>>> continuousField : numericFieldStats.entrySet()) {
            String fieldName = continuousField.getKey();
            BayesInput bayesInput = new BayesInput();
            bayesInput.setFieldName(new FieldName(fieldName));
            TargetValueStats targetValueStats = new TargetValueStats();
            for (Map.Entry<String, Map<String, Double>> valueStats : continuousField.getValue().entrySet()) {
                String className = valueStats.getKey();


                GaussianDistribution gaussianDistribution = new GaussianDistribution();
                gaussianDistribution.setMean(valueStats.getValue().get("mean"));
                gaussianDistribution.setVariance(valueStats.getValue().get("variance"));

                TargetValueStat targetValueStat = new TargetValueStat();
                targetValueStat.setValue(className);
                targetValueStat.setContinuousDistribution(gaussianDistribution);
                targetValueStats.addTargetValueStats(targetValueStat);
            }
            bayesInput.setTargetValueStats(targetValueStats);
            bayesInputs.addBayesInputs(bayesInput);
        }
        naiveBayesModel.setBayesInputs(bayesInputs);
    }

    private static void setDataDictionary(PMML pmml, TreeMap<String, TreeSet<String>> allTermsPerField,
                                          Set<String> numericFieldsNames) {

        DataDictionary dataDictionary = new DataDictionary();

        for (Map.Entry<String, TreeSet<String>> fieldNameAndTerms : allTermsPerField.entrySet()) {
            DataField dataField = new DataField();
            dataField.setName(new FieldName(fieldNameAndTerms.getKey()));
            dataField.setOpType(OpType.CATEGORICAL);
            dataField.setDataType(DataType.STRING);
            for (String term : fieldNameAndTerms.getValue()) {
                dataField.addValues(new Value(term));
            }
            dataDictionary.addDataFields(dataField);
        }

        for (String fieldname : numericFieldsNames) {
            DataField dataField = new DataField();
            dataField.setName(new FieldName(fieldname));
            dataField.setOpType(OpType.CONTINUOUS);
            // TODO: handle ints etc.
            dataField.setDataType(DataType.DOUBLE);
            dataDictionary.addDataFields(dataField);
        }
        pmml.setDataDictionary(dataDictionary);
    }

    private static void setTargetValueCounts(NaiveBayesModel naiveBayesModel, Terms classAgg, long[] classCounts, String[] classLabels) {
        TargetValueCounts targetValueCounts = new TargetValueCounts();
        for (int i = 0; i < classLabels.length; i++) {
            TargetValueCount targetValueCount = new TargetValueCount();
            targetValueCount.setValue(classLabels[i]);
            targetValueCount.setCount(classCounts[i]);
            targetValueCounts.addTargetValueCounts(targetValueCount);
        }
        naiveBayesModel.setBayesOutput(new BayesOutput().setFieldName(new FieldName(classAgg.getName())).setTargetValueCounts
                (targetValueCounts));
    }
}
