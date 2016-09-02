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

package org.elasticsearch.plugin;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.allterms.AllTermsAction;
import org.elasticsearch.action.allterms.TransportAllTermsAction;
import org.elasticsearch.action.allterms.TransportAllTermsShardAction;
import org.elasticsearch.action.preparespec.PrepareSpecAction;
import org.elasticsearch.action.preparespec.TransportPrepareSpecAction;
import org.elasticsearch.action.trainmodel.TrainModelAction;
import org.elasticsearch.action.trainmodel.TransportTrainModelAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.AnalyzerProcessor;
import org.elasticsearch.ingest.IngestAnalysisService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ml.training.ModelTrainers;
import org.elasticsearch.ml.training.NaiveBayesModelTrainer;
import org.elasticsearch.ml.training.TrainingService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.action.allterms.RestAllTermsAction;
import org.elasticsearch.rest.action.preparespec.RestPrepareSpecAction;
import org.elasticsearch.rest.action.storemodel.RestStoreModelAction;
import org.elasticsearch.rest.action.trainmodel.RestTrainModelAction;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.pmml.PMMLModelScriptEngineService;
import org.elasticsearch.script.pmml.VectorScriptFactory;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.analyzedtext.AnalyzedTextFetchSubPhase;
import org.elasticsearch.search.fetch.termvectors.TermVectorsFetchSubPhase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TokenPlugin extends Plugin implements ScriptPlugin, ActionPlugin, SearchPlugin, IngestPlugin {

    private final Settings settings;
    private final boolean transportClientMode;
    private final IngestAnalysisService ingestAnalysisService;

    public TokenPlugin(Settings settings) {
        this.settings = settings;
        this.transportClientMode = TransportClient.CLIENT_TYPE.equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
        ingestAnalysisService = new IngestAnalysisService(settings);
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               SearchRequestParsers searchRequestParsers) {
        ModelTrainers modelTrainers = new ModelTrainers(Arrays.asList(new NaiveBayesModelTrainer()));
        TrainingService trainingService = new TrainingService(settings, clusterService, client, modelTrainers, searchRequestParsers);

        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        Setting<Settings> ingestAnalysisGroupSetting = ingestAnalysisService.getIngestAnalysisGroupSetting();
        clusterSettings.addSettingsUpdateConsumer(ingestAnalysisGroupSetting, ingestAnalysisService::setAnalysisSettings);
        ingestAnalysisService.setAnalysisSettings(ingestAnalysisGroupSetting.get(settings));

        return Arrays.asList(trainingService, ingestAnalysisService);
    }

    @Override
    public ScriptEngineService getScriptEngineService(Settings settings) {
        return new PMMLModelScriptEngineService(settings);
    }

    @Override
    public List<NativeScriptFactory> getNativeScripts() {
        return Collections.singletonList(new VectorScriptFactory());
    }

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(AllTermsAction.INSTANCE, TransportAllTermsAction.class, TransportAllTermsShardAction.class),
                new ActionHandler<>(PrepareSpecAction.INSTANCE, TransportPrepareSpecAction.class),
                new ActionHandler<>(TrainModelAction.INSTANCE, TransportTrainModelAction.class)
        );
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        return Arrays.asList(
                RestAllTermsAction.class,
                RestPrepareSpecAction.class,
                RestStoreModelAction.class,
                RestTrainModelAction.class
        );
    }

    @Override
    public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
        return Arrays.asList(
                new TermVectorsFetchSubPhase(),
                new AnalyzedTextFetchSubPhase()
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        ingestAnalysisService.setAnalysisRegistry(parameters.analysisRegistry);
        return Collections.singletonMap(AnalyzerProcessor.TYPE, new AnalyzerProcessor.Factory(ingestAnalysisService));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(ingestAnalysisService.getIngestAnalysisGroupSetting());
    }
}
