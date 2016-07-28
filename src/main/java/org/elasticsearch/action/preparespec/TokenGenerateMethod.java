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

package org.elasticsearch.action.preparespec;

/**
 * Created by britta on 03.03.16.
 */
public enum TokenGenerateMethod {
    GIVEN,
    SIGNIFICANT_TERMS,
    ALL_TERMS;

    public String toString() {
        switch (this.ordinal()) {
            case 0:
                return "given";
            case 1:
                return "significant_terms";
            case 2:
                return "all_terms";
        }
        throw new IllegalStateException("There is no toString() for ordinal " + this.ordinal() +
                " - someone forgot to implement toString().");
    }

    public static TokenGenerateMethod fromString(String s) {
        if (s.equals(GIVEN.toString())) {
            return GIVEN;
        } else if (s.equals(SIGNIFICANT_TERMS.toString())) {
            return SIGNIFICANT_TERMS;
        } else if (s.equals(ALL_TERMS.toString())) {
            return ALL_TERMS;
        } else {
            throw new IllegalStateException("Don't know what " + s + " is - choose one of " + GIVEN.toString() + " " +
                    SIGNIFICANT_TERMS.toString() + " " + ALL_TERMS.toString() + " ");
        }
    }
}
