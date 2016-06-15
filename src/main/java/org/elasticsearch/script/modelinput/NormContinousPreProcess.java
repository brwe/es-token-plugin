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

package org.elasticsearch.script.modelinput;

import org.dmg.pmml.NormContinuous;

public class NormContinousPreProcess extends PreProcessingStep {
    double factor;
    double b1;
    double a1;

    public NormContinousPreProcess(NormContinuous expression, String derivedFieldName) {
        super(derivedFieldName);
        if (expression.getLinearNorms().size() != 2) {
            throw new UnsupportedOperationException("Linear norms with more or less than two components not implemented yet!");

        }
        b1 = expression.getLinearNorms().get(0).getNorm();
        double b2 = expression.getLinearNorms().get(1).getNorm();
        a1 = expression.getLinearNorms().get(0).getOrig();
        double a2 = expression.getLinearNorms().get(1).getOrig();
        // see http://dmg.org/pmml/v4-2-1/Transformations.html#xsdElement_NormContinuous  b1+ ( x-a1)/(a2-a1)*(b2-b1)
        factor = (b2 - b1) / (a2 - a1);
    }

    @Override
    public Object apply(Object o) {
        assert o instanceof Number;
        return b1 + (((Number) o).doubleValue() - a1) * factor;
    }
}
