/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.plugin.mapper.token;

import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.allterms.AllTermsAction;
import org.elasticsearch.action.allterms.TransportAllTermsAction;
import org.elasticsearch.action.allterms.TransportAllTermsShardAction;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.rest.action.allterms.RestAllTermsAction;
import org.elasticsearch.script.NaiveBayesModelScript;
import org.elasticsearch.script.SVMModelScript;
import org.elasticsearch.script.VectorizerScript;
import org.elasticsearch.script.ScriptModule;

import java.util.Collection;

import static org.elasticsearch.common.collect.Lists.newArrayList;

/**
 *
 */
public class AnalyzedTextPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "analyzed-text";
    }

    @Override
    public String description() {
        return "Adds the analyzed_text type that stores the analyzed text in a separate field";
    }

    @Override
    public Collection<Class<? extends Module>> indexModules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        modules.add(AnalyzedTextIndexModule.class);
        return modules;
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof ActionModule) {
            ActionModule actionModule = (ActionModule) module;
            actionModule.registerAction(AllTermsAction.INSTANCE, TransportAllTermsAction.class,
                    TransportAllTermsShardAction.class);
        } else if (module instanceof RestModule) {
            RestModule restModule = (RestModule) module;
            restModule.addRestAction(RestAllTermsAction.class);
        }
    }

    public void onModule(ScriptModule module) {
        // Register each script that we defined in this plugin
        module.registerScript(VectorizerScript.SCRIPT_NAME, VectorizerScript.Factory.class);
        module.registerScript(NaiveBayesModelScript.SCRIPT_NAME, NaiveBayesModelScript.Factory.class);
        module.registerScript(SVMModelScript.SCRIPT_NAME, SVMModelScript.Factory.class);
    }
}
