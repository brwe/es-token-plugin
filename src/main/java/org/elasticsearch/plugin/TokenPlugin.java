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
import org.elasticsearch.index.mapper.token.AnalyzedTextFieldMapper;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.rest.action.allterms.RestAllTermsAction;
import org.elasticsearch.script.LogisticRegressionModelScriptWithStoredParametersAndSparseVector;
import org.elasticsearch.script.NaiveBayesModelScriptWithStoredParameters;
import org.elasticsearch.script.NaiveBayesModelScriptWithStoredParametersAndSparseVector;
import org.elasticsearch.script.NaiveBayesUpdateScript;
import org.elasticsearch.script.PMMLScriptWithStoredParametersAndSparseVector;
import org.elasticsearch.script.SVMModelScriptWithStoredParameters;
import org.elasticsearch.script.SVMModelScriptWithStoredParametersAndSparseVector;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.SparseVectorizerScript;
import org.elasticsearch.script.VectorizerScript;

/**
 *
 */
public class TokenPlugin extends Plugin {

    @Override
    public String name() {
        return "token-plugin";
    }

    @Override
    public String description() {
        return "Tools for https://github.com/costin/poc";
    }


    public void onModule(ScriptModule module) {
        // Register each script that we defined in this plugin
        module.registerScript(VectorizerScript.SCRIPT_NAME, VectorizerScript.Factory.class);
        module.registerScript(SparseVectorizerScript.SCRIPT_NAME, SparseVectorizerScript.Factory.class);
        module.registerScript(NaiveBayesModelScriptWithStoredParameters.SCRIPT_NAME, NaiveBayesModelScriptWithStoredParameters.Factory.class);
        module.registerScript(SVMModelScriptWithStoredParameters.SCRIPT_NAME, SVMModelScriptWithStoredParameters.Factory.class);
        module.registerScript(NaiveBayesModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME, NaiveBayesModelScriptWithStoredParametersAndSparseVector.Factory.class);
        module.registerScript(SVMModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME, SVMModelScriptWithStoredParametersAndSparseVector.Factory.class);
        module.registerScript(NaiveBayesUpdateScript.SCRIPT_NAME, NaiveBayesUpdateScript.Factory.class);
        module.registerScript(PMMLScriptWithStoredParametersAndSparseVector.SCRIPT_NAME, PMMLScriptWithStoredParametersAndSparseVector.Factory.class);
        module.registerScript(LogisticRegressionModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME, LogisticRegressionModelScriptWithStoredParametersAndSparseVector.Factory.class);
    }

    public void onModule(ActionModule module) {
        ActionModule actionModule = (ActionModule) module;
        actionModule.registerAction(AllTermsAction.INSTANCE, TransportAllTermsAction.class,
                TransportAllTermsShardAction.class);
    }

    public void onModule(RestModule module) {
        RestModule restModule = (RestModule) module;
        restModule.addRestAction(RestAllTermsAction.class);
    }
    public void onModule(IndicesModule indicesModule) {
        indicesModule.registerMapper(AnalyzedTextFieldMapper.CONTENT_TYPE, new AnalyzedTextFieldMapper.TypeParser());
    }
}
