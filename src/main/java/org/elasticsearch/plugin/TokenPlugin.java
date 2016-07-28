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

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.allterms.AllTermsAction;
import org.elasticsearch.action.allterms.TransportAllTermsAction;
import org.elasticsearch.action.allterms.TransportAllTermsShardAction;
import org.elasticsearch.action.preparespec.PrepareSpecAction;
import org.elasticsearch.action.preparespec.TransportPrepareSpecAction;
import org.elasticsearch.action.trainnaivebayes.TrainNaiveBayesAction;
import org.elasticsearch.action.trainnaivebayes.TransportTrainNaiveBayesAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.action.allterms.RestAllTermsAction;
import org.elasticsearch.rest.action.preparespec.RestPrepareSpecAction;
import org.elasticsearch.rest.action.storemodel.RestStoreModelAction;
import org.elasticsearch.rest.action.trainnaivebayes.RestTrainNaiveBayesAction;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.pmml.PMMLModelScriptEngineService;
import org.elasticsearch.script.pmml.VectorScriptFactory;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.fetch.analyzedtext.AnalyzedTextFetchSubPhase;
import org.elasticsearch.search.fetch.termvectors.TermVectorsFetchSubPhase;

import java.util.Collections;
import java.util.List;

/**
 *
 */
public class TokenPlugin extends Plugin implements ScriptPlugin {

    private final Settings settings;
    private final boolean transportClientMode;


    public TokenPlugin(Settings settings) {
        this.settings = settings;
        this.transportClientMode = TransportClient.CLIENT_TYPE.equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));;
    }

    @Override
    public ScriptEngineService getScriptEngineService(Settings settings) {
        return new PMMLModelScriptEngineService(settings);
    }

    @Override
    public List<NativeScriptFactory> getNativeScripts() {
        return Collections.singletonList(new VectorScriptFactory());
    }

    //TODO: switch to ActionScript after 5.0.0-beta4
    public void onModule(ActionModule module) {
        module.registerAction(AllTermsAction.INSTANCE, TransportAllTermsAction.class,
                TransportAllTermsShardAction.class);
        module.registerAction(PrepareSpecAction.INSTANCE, TransportPrepareSpecAction.class);
        module.registerAction(TrainNaiveBayesAction.INSTANCE, TransportTrainNaiveBayesAction.class);
    }

    //TODO: switch to ActionScript after 5.0.0-beta4
    public void onModule(NetworkModule module) {
        if (!transportClientMode) {
            module.registerRestHandler(RestAllTermsAction.class);
            module.registerRestHandler(RestPrepareSpecAction.class);
            module.registerRestHandler(RestStoreModelAction.class);
            module.registerRestHandler(RestTrainNaiveBayesAction.class);
        }
    }

    public void onModule(SearchModule searchModule) {
        searchModule.registerFetchSubPhase(new TermVectorsFetchSubPhase());
        searchModule.registerFetchSubPhase(new AnalyzedTextFetchSubPhase());
    }
}
