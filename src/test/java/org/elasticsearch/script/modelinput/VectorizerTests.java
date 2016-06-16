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

import org.elasticsearch.action.preparespec.FieldSpec;
import org.elasticsearch.action.preparespec.StringFieldSpec;
import org.elasticsearch.action.preparespec.TransportPrepareSpecAction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class VectorizerTests extends ESTestCase {

    public void testVectorizerParsing() throws IOException {
        VectorRangesToVector entries = new VectorRangesToVectorJSON(createSpecSource());
        assertParameters(entries);
    }

    public void testVectorizerParsingFromActualSource() throws IOException {
        VectorRangesToVector entries = new VectorRangesToVectorJSON(createSpecSourceFromSpec());
        assertParameters(entries);
    }

    private Map<String, Object> createSpecSourceFromSpec() throws IOException {
        List<FieldSpec> specs= new ArrayList<>();
        specs.add(new StringFieldSpec( new String[]{"a", "b", "c"}, "tf", "text1"));
        specs.add(new StringFieldSpec( new String[]{"d", "e", "f"}, "occurrence", "text2"));
        Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(TransportPrepareSpecAction.FieldSpecActionListener.createSpecSource(specs, false, 6).bytes());
        String script = (String)sourceAsMap.get("script");
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(script);
        return parser.mapOrdered();
    }

    public void assertParameters(VectorRangesToVector entries) {
        assertThat(entries.sparse, equalTo(false));
        assertThat(entries.vectorRangeList.get(0), instanceOf(AnalyzedTextVectorRange.DenseTermVectorRange.class));
        assertThat(entries.vectorRangeList.get(1), instanceOf(AnalyzedTextVectorRange.DenseTermVectorRange.class));
        AnalyzedTextVectorRange.DenseTermVectorRange entry1 = (AnalyzedTextVectorRange.DenseTermVectorRange) entries.vectorRangeList.get(0);
        AnalyzedTextVectorRange.DenseTermVectorRange entry2 = (AnalyzedTextVectorRange.DenseTermVectorRange) entries.vectorRangeList.get(1);
        assertThat(entry1, instanceOf(AnalyzedTextVectorRange.DenseTermVectorRange.class));
        assertThat(entry2, instanceOf(AnalyzedTextVectorRange.DenseTermVectorRange.class));
        assertThat(entry1.size(), equalTo(3));
        assertThat(entry2.size(), equalTo(3));
        assertThat(entry1.offset, equalTo(0));
        assertThat(entry2.offset, equalTo(3));
        assertThat(entry2.field, equalTo("text2"));
        assertThat(entry1.field, equalTo("text1"));
        assertThat(entry2.number, equalTo("occurrence"));
        assertThat(entry1.number, equalTo("tf"));
        assertArrayEquals(entry1.terms,  new String[]{"a", "b", "c"} );
        assertArrayEquals(entry2.terms,  new String[]{"d", "e", "f"} );
    }


    protected static Map<String, Object> createSpecSource() throws IOException {
        XContentBuilder request = jsonBuilder();

        request.startObject()
                .startArray("features")
                .startObject()
                .field("field", "text1")
                .field("type", "terms")
                .field("terms", new String[]{"a", "b", "c"})
                .field("number", "tf")
                .endObject()
                .startObject()
                .field("field", "text2")
                .field("type", "terms")
                .field("terms", new String[]{"d", "e", "f"})
                .field("number", "occurrence")
                .endObject()
                .endArray()
                .field("sparse", false)
                .endObject();
        return SourceLookup.sourceAsMap(request.bytes());
    }


}
