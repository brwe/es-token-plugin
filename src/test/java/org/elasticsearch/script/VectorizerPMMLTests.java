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

package org.elasticsearch.script;

import org.dmg.pmml.PMML;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import java.io.*;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.CoreMatchers.equalTo;

public class VectorizerPMMLTests extends ESTestCase {

    public void testVectorizerParsing() throws IOException {
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/logistic_regression.xml");
        PMML pmml = AccessController.doPrivileged(new PrivilegedAction<PMML>() {
            public PMML run() {
                try (InputStream is = new ByteArrayInputStream(pmmlString.getBytes(Charset.defaultCharset()))) {
                    Source transformedSource = ImportFilter.apply(new InputSource(is));
                    return JAXBUtil.unmarshalPMML(transformedSource);
                } catch (SAXException e) {
                    throw new ElasticsearchException("could not convert xml to pmml model", e);
                } catch (JAXBException e) {
                    throw new ElasticsearchException("could not convert xml to pmml model", e);
                } catch (IOException e) {
                    throw new ElasticsearchException("could not convert xml to pmml model", e);
                }
            }
        });
        pmml.getDataDictionary();

        VectorEntries vectorEntries = new VectorEntries(pmml, 0);
        assertThat(vectorEntries.features.size(), equalTo(10));
    }

    public void testVectorizerParsingWithDOM() throws IOException, SAXException, ParserConfigurationException {

        DocumentBuilderFactory factory =
                DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/logistic_regression.xml");

        Document doc = builder.parse(new ByteArrayInputStream(pmmlString.getBytes(Charset.defaultCharset())));
        NodeList nodeList = doc.getElementsByTagName("PPCell");
        for (int i = 0; i< nodeList.getLength(); i++) {
            logger.info("Found node {}", nodeList.item(i).getLocalName());
        }
        logger.info("        One more time:");
        nodeList = doc.getElementsByTagName("PPCell");
        for (int i = 0; i< nodeList.getLength(); i++) {
            logger.info("Found node {}", nodeList.item(i).getLocalName());
        }
        logger.info("        MiningSchema:");
        doc.getElementsByTagNameNS("GeneralRegressionModel", "MiningSchema");

        Node model = doc.getElementById("GeneralRegressionModel");

    }


}
