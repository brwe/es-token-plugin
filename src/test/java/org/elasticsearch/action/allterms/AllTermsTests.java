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

package org.elasticsearch.action.allterms;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NoMergeScheduler;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.allterms.TransportAllTermsShardAction.getTermsEnums;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.IsEqual.equalTo;

public class AllTermsTests extends ESTestCase {

    private static final String FIELD = "field";
    private Directory dir;
    private IndexWriter w;
    private DirectoryReader reader;

    @Before
    public void initSearcher() throws IOException {
        dir = newDirectory();
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig(new WhitespaceAnalyzer());
        indexWriterConfig.setMergeScheduler(NoMergeScheduler.INSTANCE);
        indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        w = new IndexWriter(dir, indexWriterConfig);
        Document d = new Document();
        d.add(new TextField(FIELD, "don't  be", Field.Store.YES));
        d.add(new TextField("_uid", "1", Field.Store.YES));
        w.addDocument(d);
        w.commit();
        d = new Document();
        d.add(new TextField(FIELD, "ever always forget  be", Field.Store.YES));
        d.add(new TextField("_uid", "2", Field.Store.YES));
        w.addDocument(d);
        w.commit();
        d = new Document();
        d.add(new TextField(FIELD, "careful careful", Field.Store.YES));
        d.add(new TextField("_uid", "3", Field.Store.YES));
        w.addDocument(d);
        w.commit();
        d = new Document();
        d.add(new TextField(FIELD, "ever always careful careful don't be forget be", Field.Store.YES));
        d.add(new TextField("_uid", "4", Field.Store.YES));
        w.addDocument(d);
        w.commit();
        reader = DirectoryReader.open(w, true);
    }

    @After
    public void closeAllTheReaders() throws IOException {

        w.close();
        reader.close();
        dir.close();
    }

    public void testReader() {
        assertThat(reader.leaves().size(), equalTo(4));
    }

    public void testFindSmallestTermAfterExistingTerm() throws IOException {
        SmallestTermAndExhausted smallestTermAndExhausted = getSmallestTermAndExhausted("careful");
        BytesRef smallestTerm = smallestTermAndExhausted.getSmallestTerm();
        int[] exhausted = smallestTermAndExhausted.getExhausted();
        assertThat(smallestTerm.utf8ToString(), equalTo("don't"));
        int i = -1;
        int numExhausted = 0;
        for (TermsEnum termsEnum : smallestTermAndExhausted.getTermsIters()) {
            i++;
            if (exhausted[i] == 1) {
                numExhausted++;
            } else {
                assertThat(termsEnum.term().utf8ToString().compareTo("careful"), greaterThan(0));
            }
        }
        assertThat(numExhausted, equalTo(1));
    }


    public void testFindSmallestTermFromBeginning() throws IOException {
        SmallestTermAndExhausted smallestTermAndExhausted = getSmallestTermAndExhausted(null);
        BytesRef smallestTerm = smallestTermAndExhausted.getSmallestTerm();
        int[] exhausted = smallestTermAndExhausted.getExhausted();
        assertThat(smallestTerm.utf8ToString(), equalTo("always"));
        int i = -1;
        int numExhausted = 0;
        for (TermsEnum termsEnum : smallestTermAndExhausted.getTermsIters()) {
            i++;
            if (exhausted[i] == 1) {
                numExhausted++;
            } else {
                assertThat(termsEnum.term().utf8ToString().compareTo("always"), greaterThanOrEqualTo(0));
            }
        }
        assertThat(numExhausted, equalTo(0));
    }

    public void testFindSmallestTermFromNotExistentTerm() throws IOException {
        SmallestTermAndExhausted smallestTermAndExhausted = getSmallestTermAndExhausted("foo");
        BytesRef smallestTerm = smallestTermAndExhausted.getSmallestTerm();
        int[] exhausted = smallestTermAndExhausted.getExhausted();
        assertThat(smallestTerm.utf8ToString(), equalTo("forget"));
        int i = -1;
        int numExhausted = 0;
        for (TermsEnum termsEnum : smallestTermAndExhausted.getTermsIters()) {
            i++;
            if (exhausted[i] == 1) {
                numExhausted++;
            } else {
                assertThat(termsEnum.term().utf8ToString().compareTo("foo"), greaterThanOrEqualTo(0));
            }
        }
        assertThat(numExhausted, equalTo(2));
    }

    public void testFindSmallestAllExhausted() throws IOException {
        SmallestTermAndExhausted smallestTermAndExhausted = getSmallestTermAndExhausted("zonk");
        BytesRef smallestTerm = smallestTermAndExhausted.getSmallestTerm();
        int[] exhausted = smallestTermAndExhausted.getExhausted();
        assertThat(smallestTerm, equalTo(null));
        for (int i = 0; i < 4; i++) {
            assertThat(exhausted[i], equalTo(1));
        }
    }

    private SmallestTermAndExhausted getSmallestTermAndExhausted(String from) throws IOException {
        AllTermsShardRequest request = new AllTermsShardRequest(new AllTermsRequest(), "index", 0, "field", 1, from, 0);
        List<TermsEnum> termIters = getTermsEnums(request, reader.leaves());
        assertThat(termIters.size(), equalTo(4));
        BytesRef smallestTerm = null;
        int[] exhausted = new int[termIters.size()];
        smallestTerm = TransportAllTermsShardAction.findSmallestTermAfter(request, termIters, smallestTerm, exhausted);
        return new SmallestTermAndExhausted(smallestTerm, exhausted, termIters);
    }

    private class SmallestTermAndExhausted {
        private BytesRef smallestTerm;
        private int[] exhausted;
        private List<TermsEnum> termsIters;

        public BytesRef getSmallestTerm() {
            return smallestTerm;
        }

        public int[] getExhausted() {
            return exhausted;
        }

        SmallestTermAndExhausted(BytesRef smallestTerm, int[] exhausted, List<TermsEnum> termsIters) {

            this.smallestTerm = smallestTerm;
            this.exhausted = exhausted;
            this.termsIters = termsIters;
        }

        public List<TermsEnum> getTermsIters() {
            return termsIters;
        }
    }

    public void testDocFreqForExistingTerm() throws IOException {
        SmallestTermAndExhausted smallestTermAndExhausted = getSmallestTermAndExhausted("careful");
        BytesRef smallestTerm = smallestTermAndExhausted.getSmallestTerm();
        int[] exhausted = smallestTermAndExhausted.getExhausted();
        assertThat(TransportAllTermsShardAction.getDocFreq(smallestTermAndExhausted.getTermsIters(), smallestTerm, exhausted), equalTo(2l));
    }

    public void testDocFreqForNotExistingTerm() throws IOException {
        SmallestTermAndExhausted smallestTermAndExhausted = getSmallestTermAndExhausted("careful");
        BytesRef smallestTerm = new BytesRef("do");
        int[] exhausted = smallestTermAndExhausted.getExhausted();
        assertThat(TransportAllTermsShardAction.getDocFreq(smallestTermAndExhausted.getTermsIters(), smallestTerm, exhausted), equalTo(0l));
    }

    public void testMoveIterators() throws IOException {
        SmallestTermAndExhausted smallestTermAndExhausted = getSmallestTermAndExhausted("a");
        BytesRef smallestTerm = new BytesRef(smallestTermAndExhausted.getSmallestTerm().utf8ToString());
        TransportAllTermsShardAction.moveIterators(smallestTermAndExhausted.exhausted, smallestTermAndExhausted.getTermsIters(), smallestTerm);
        for (int i = 0; i < 4; i++) {
            assertThat(smallestTermAndExhausted.getTermsIters().get(i).term(), greaterThan(smallestTerm));
        }
    }

    public void testMoveIteratorsWithSomeExhaustion() throws IOException {
        SmallestTermAndExhausted smallestTermAndExhausted = getSmallestTermAndExhausted("careful");
        BytesRef smallestTerm = new BytesRef(smallestTermAndExhausted.getSmallestTerm().utf8ToString());
        TransportAllTermsShardAction.moveIterators(smallestTermAndExhausted.exhausted, smallestTermAndExhausted.getTermsIters(), smallestTerm);
        int exhausted = 0;
        for (int i = 0; i < 4; i++) {
            if (smallestTermAndExhausted.getExhausted()[i] != 1) {
                assertThat(smallestTermAndExhausted.getTermsIters().get(i).term(), greaterThan(smallestTerm));
            } else {
                exhausted++;
            }
        }
        assertThat(exhausted, equalTo(2));
    }

    public void testFindSmallestTerm() throws IOException {
        SmallestTermAndExhausted smallestTermAndExhausted = getSmallestTermAndExhausted("careful");
        BytesRef smallestTerm = new BytesRef(smallestTermAndExhausted.getSmallestTerm().utf8ToString());
        BytesRef newSmallestTerm = TransportAllTermsShardAction.findMinimum(smallestTermAndExhausted.exhausted, smallestTermAndExhausted.getTermsIters());
        assertThat(newSmallestTerm.utf8ToString(), equalTo(smallestTerm.utf8ToString()));
    }

    public void testGetAllTermsFromBeginning() throws IOException {
        AllTermsShardRequest request = new AllTermsShardRequest(new AllTermsRequest(), "index", 0, "field", 10, null, 0);
        List<String> terms = new ArrayList<>();
        TransportAllTermsShardAction.getTerms(request, terms, reader.leaves());
        assertArrayEquals(terms.toArray(new String[6]), new String[]{"always", "be", "careful", "don't", "ever", "forget"});
    }

    public void testGetAllTermsFromBeginningExact() throws IOException {
        AllTermsShardRequest request = new AllTermsShardRequest(new AllTermsRequest(), "index", 0, "field", 6, null, 0);
        List<String> terms = new ArrayList<>();
        TransportAllTermsShardAction.getTerms(request, terms, reader.leaves());
        assertArrayEquals(terms.toArray(new String[6]), new String[]{"always", "be", "careful", "don't", "ever", "forget"});
    }

    public void testGetSomeTerms() throws IOException {
        AllTermsShardRequest request = new AllTermsShardRequest(new AllTermsRequest(), "index", 0, "field", 3, null, 0);
        List<String> terms = new ArrayList<>();
        TransportAllTermsShardAction.getTerms(request, terms, reader.leaves());
        assertArrayEquals(terms.toArray(new String[3]), new String[]{"always", "be", "careful"});
    }

    public void testGetSomeTermsFrom() throws IOException {
        AllTermsShardRequest request = new AllTermsShardRequest(new AllTermsRequest(), "index", 0, "field", 3, "careful", 0);
        List<String> terms = new ArrayList<>();
        TransportAllTermsShardAction.getTerms(request, terms, reader.leaves());
        assertArrayEquals(terms.toArray(new String[3]), new String[]{"don't", "ever", "forget"});
    }
}
