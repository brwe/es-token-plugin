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

package org.elasticsearch.index.mapper.token;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class AnalyzedTextFieldMapperTests extends ESSingleNodeTestCase {

    MapperRegistry mapperRegistry;
    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void before() {
        indexService = createIndex("test");
        mapperRegistry = new MapperRegistry(
                Collections.<String, Mapper.TypeParser>singletonMap(AnalyzedTextFieldMapper.CONTENT_TYPE, new AnalyzedTextFieldMapper.TypeParser()),
                Collections.<String, MetadataFieldMapper.TypeParser>emptyMap());
        parser = new DocumentMapperParser(indexService.indexSettings(), indexService.mapperService(),
                indexService.analysisService(), indexService.similarityService().similarityLookupService(),
                null, mapperRegistry);
    }

    public void testDefaults() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "analyzed_text")
                .endObject().endObject().endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        FieldMapper analyzedTextMapper = mapper.mappers().getMapper("field");
        assertThat(analyzedTextMapper.fieldType().hasDocValues(), equalTo(true));
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertThat(builder.string(), equalTo(mapping));
    }
}
