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

import org.dmg.pmml.Constant;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.DerivedField;

public class MissingValuePreProcess extends PreProcessingStep {

    private Object missingValue;

    public MissingValuePreProcess(DerivedField derivedField, String missingValue) {
        super(derivedField.getName().getValue());
        this.missingValue = parseMissingValue(derivedField.getDataType(), missingValue);
    }

    private Object parseMissingValue(DataType dataType, String missingValue) {
        Object parsedMissingValue;
        if (dataType.equals(DataType.DOUBLE)) {
            parsedMissingValue = Double.parseDouble(missingValue);
        } else if (dataType.equals(DataType.FLOAT)) {
            parsedMissingValue = Float.parseFloat(missingValue);
        } else if (dataType.equals(DataType.INTEGER)) {
            parsedMissingValue = Integer.parseInt(missingValue);
        } else if (dataType.equals(DataType.STRING)) {
            parsedMissingValue = missingValue;
        } else {
            throw new UnsupportedOperationException("Only implemented data type double, float and int so " +
                    "far.");
        }
        return parsedMissingValue;
    }

    public MissingValuePreProcess(DataField dataField, String missingValue) {
        super(dataField.getName().getValue());
        this.missingValue = parseMissingValue(dataField.getDataType(), missingValue);
    }

    @Override
    public Object apply(Object value) {
        if (value == null) {
            return missingValue;
        } else {
            return value;
        }
    }
}
