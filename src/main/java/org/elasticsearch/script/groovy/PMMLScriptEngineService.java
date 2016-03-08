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

package org.elasticsearch.script.groovy;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides the infrastructure for Groovy as a scripting language for Elasticsearch
 */
public class PMMLScriptEngineService extends NativeScriptEngineService {

    public static final String NAME = "pmml";
    private Node node;

    @Inject
    public PMMLScriptEngineService(Settings settings, Node node) {
        super(settings, new HashMap<String, NativeScriptFactory>());
        this.node = node;
    }

    @Override
    public String[] types() {
        return new String[]{NAME};
    }

    @Override
    public String[] extensions() {
        return new String[0];
    }

    @Override
    public boolean sandboxed() {
        return false;
    }

    @Override
    public Object compile(String script) {
        if (script.equals("vector")) {
            return new VectorizerScript.Factory(node);
        }
        if (script.equals("model")) {
            return new PMMLModel.Factory(node);
        }
        throw new IllegalArgumentException("PMML script [" + script + "] not found");
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
        if (compiledScript.compiled() instanceof VectorizerScript.Factory) {
            return ((VectorizerScript.Factory) compiledScript.compiled()).newScript(vars);
        }
        if (compiledScript.compiled() instanceof PMMLModel.Factory) {
            return ((PMMLModel.Factory) compiledScript.compiled()).newScript(vars);
        }
        throw new IllegalArgumentException("PMML script [" + compiledScript.compiled() + "] not found");
    }

    @Override
    public void close() {
    }

    @Override
    public void scriptRemoved(CompiledScript script) {
        // Nothing to do here
    }
}

