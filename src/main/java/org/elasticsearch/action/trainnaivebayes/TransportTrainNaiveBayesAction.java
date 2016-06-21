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

package org.elasticsearch.action.trainnaivebayes;

import org.dmg.pmml.BayesInputs;
import org.dmg.pmml.BayesOutput;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.NaiveBayesModel;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.TargetValueCount;
import org.dmg.pmml.TargetValueCounts;
import org.dmg.pmml.Value;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.SharedMethods;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jpmml.model.JAXBUtil;

import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

public class TransportTrainNaiveBayesAction extends HandledTransportAction<TrainNaiveBayesRequest, TrainNaiveBayesResponse> {

    private Client client;
    private ClusterService clusterService;

    @Inject
    public TransportTrainNaiveBayesAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                          ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver, Client client, ClusterService
                                                  clusterService) {
        super(settings, TrainNaiveBayesAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                TrainNaiveBayesRequest.class);
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(final TrainNaiveBayesRequest request, final ActionListener<TrainNaiveBayesResponse> listener) {
        AggregationBuilder aggregationBuilder = null;
        try {
            aggregationBuilder = parseNaiveBayesTrainRequests(request.source());
        } catch (IOException e) {
            listener.onFailure(e);
        }

        final NaiveBayesTrainingActionListener naiveBayesTrainingActionListener = new NaiveBayesTrainingActionListener(listener, client);
        client.prepareSearch().addAggregation(aggregationBuilder).execute(naiveBayesTrainingActionListener);
    }

    AggregationBuilder parseNaiveBayesTrainRequests(String source) throws IOException {
        Map<String, Object> parsedSource = SharedMethods.getSourceAsMap(source);
        if (parsedSource.get("fields") == null) {
            throw new ElasticsearchException("fields are missing for naive bayes training");
        }
        if (parsedSource.get("target_field") == null) {
            throw new ElasticsearchException("target_field is missing for naive bayes training");
        }

        if (parsedSource.get("index") == null) {
            throw new ElasticsearchException("index is missing for naive bayes training");
        }

        String targetField = (String) parsedSource.get("target_field");
        String index = (String) parsedSource.get("index");
        String type = (String) parsedSource.get("type");
        List<String> fields = (List<String>) parsedSource.get("fields");
        TermsBuilder topLevelClassAgg = terms("class");
        topLevelClassAgg.field(targetField);
        topLevelClassAgg.size(Integer.MAX_VALUE);
        topLevelClassAgg.shardMinDocCount(1);
        topLevelClassAgg.minDocCount(1);
        topLevelClassAgg.order(Terms.Order.term(true));
        Map fieldMappings = (Map) clusterService.state().getMetaData().getIndices().get(index).mapping(type).sourceAsMap().get
                ("properties");
        for (String field : fields) {
            Map attributes = (Map) fieldMappings.get(field);
            String fieldType = (String) attributes.get("type");
            if (fieldType.equals("string")) {
                topLevelClassAgg.subAggregation(terms(field).field(field).size(Integer.MAX_VALUE).shardMinDocCount(1).minDocCount(1)
                        .order(Terms.Order.term(true)));
            } else if (fieldType.equals("double") || fieldType.equals("float") || fieldType.equals("integer") || fieldType.equals("long")) {
                topLevelClassAgg.subAggregation(extendedStats(field).field(field));
            } else {
                throw new UnsupportedOperationException("have not implemented naive bayes training for anything but number and string " +
                        "field yet");
            }
        }
        return topLevelClassAgg;
    }

    public static class NaiveBayesTrainingActionListener implements ActionListener<SearchResponse> {

        private ActionListener<TrainNaiveBayesResponse> listener;
        final private Client client;

        public NaiveBayesTrainingActionListener(ActionListener<TrainNaiveBayesResponse> listener, Client client) {
            this.listener = listener;
            this.client = client;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
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
            setTargetValueCounts(naiveBayesModel, classAgg, classCounts, classLabels);

            TreeMap<String, TreeMap<String,TreeMap<String, Long>>> stringFieldValueCounts = new TreeMap<>();
            TreeMap<String, TreeSet<String>> allTermsPerField = new TreeMap<>();
            TreeMap<String, TreeMap<String, Map<String, Double>>> numericFieldStats = new TreeMap<>();
            for (Terms.Bucket bucket : classAgg.getBuckets()) {
                String className = bucket.getKeyAsString();
                for (Aggregation aggregation : bucket.getAggregations()) {
                    String fieldName = aggregation.getName();
                    if (aggregation instanceof Terms) {
                        Terms termAgg = (Terms) aggregation;
                        if (stringFieldValueCounts.containsKey(fieldName) == false) {
                            stringFieldValueCounts.put(fieldName, new TreeMap<String, TreeMap<String, Long>>());
                            allTermsPerField.put(fieldName, new TreeSet<String>());
                        }
                        TreeMap<String, Long> termCounts = new TreeMap<>();

                        for (Terms.Bucket termBucket : termAgg.getBuckets()) {
                            allTermsPerField.get(fieldName).add(termBucket.getKeyAsString());
                            termCounts.put(termBucket.getKeyAsString(), termBucket.getDocCount());
                        }
                        stringFieldValueCounts.get(fieldName).put(className, termCounts);
                    } else if (aggregation instanceof ExtendedStats) {
                        ExtendedStats extendedStats = (ExtendedStats) aggregation;
                        if (numericFieldStats.get(fieldName) == null) {
                            numericFieldStats.put(fieldName, new TreeMap<String, Map<String, Double>>());
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
            setBayesInputs(naiveBayesModel, stringFieldValueCounts, numericFieldStats);


            final PMML pmml = new PMML();
            setDataDictionary(pmml, allTermsPerField, numericFieldStats.keySet());


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
            String pmmlString = new String(outputStream.toByteArray());
            int i = 0;
        }

        private void setBayesInputs(NaiveBayesModel naiveBayesModel, TreeMap<String, TreeMap<String, TreeMap<String, Long>>> stringFieldValueCounts,
                                    TreeMap<String, TreeMap<String, Map<String, Double>>> numericFieldStats) {
            BayesInputs bayesInputs = new BayesInputs();
            //for (Map.Entry <String, TreeMap<String, TreeMap<String, Long>> )
        }

        private static void setDataDictionary(PMML pmml, TreeMap<String, TreeSet<String>> allTermsPerField, Set<String> numericFieldsNames) {

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

        @Override
        public void onFailure(Throwable throwable) {

            listener.onFailure(throwable);
        }
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
