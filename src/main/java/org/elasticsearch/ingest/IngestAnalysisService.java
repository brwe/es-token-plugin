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

package org.elasticsearch.ingest;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalysisService;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service for maintaining currently defined custom analyzers for the analyzer ingest processor
 */
public class IngestAnalysisService extends AbstractLifecycleComponent {

    private final Setting<Settings> ingestAnalysisGroupSetting;
    
    // injected later on during plugin initialization
    private AnalysisRegistry analysisRegistry;

    // updated dynamically on settings change
    private AnalysisServiceHolder analysisServiceHolder;
    private final Object holderLock = new Object();

    public IngestAnalysisService(Settings settings) {
        super(settings);
        ingestAnalysisGroupSetting = Setting.groupSetting("ingest.analysis.", this::validateSettings, Setting.Property.Dynamic,
                Setting.Property.NodeScope);
    }


    public Setting<Settings> getIngestAnalysisGroupSetting() {
        return ingestAnalysisGroupSetting;
    }

    public void setAnalysisRegistry(AnalysisRegistry analysisRegistry) {
        assert this.analysisRegistry == null && analysisRegistry != null; // shouldn't initialize more then once
        this.analysisRegistry = analysisRegistry;
    }

    
    public void validateSettings(Settings analysis) {
        // During node startup the analysisRegistry registry is null
        // That's fine - we will re-validate the settings a bit later when we build the initial analysis service anyway
        if (analysisRegistry != null && analysis.isEmpty() == false) {
            // We only need to test in case of custom settings
            buildAnalysisService(analysis).close();
        }
    }

    /**
     * Replaces the internal analysis service with the new version.
     * This method is called during initialization and when analysis settings are dynamically changed
     */
    public void setAnalysisSettings(Settings analysis) {
        setAnalysisServiceHolder(new AnalysisServiceHolder(buildAnalysisService(analysis)));
    }

    /**
     * Builds an analysis service based on the specified analysis settings
     */
    private AnalysisService buildAnalysisService(Settings analysis) {
        assert analysisRegistry != null;
        Settings settings = getAnonymousSettings(Settings.builder().put(analysis).normalizePrefix("index.analysis.").build());
        IndexSettings indexSettings = getNaIndexSettings(settings);
        try {
            return analysisRegistry.build(indexSettings);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Couldn't parse ingest analyzer definition [" + analysis.toDelimitedString(',') + "]", ex);
        }
    }

    /**
     * Sets the new analysis holder in a thread-safe manner
     */
    private void setAnalysisServiceHolder(AnalysisServiceHolder analysisServiceHolder) {
        synchronized (holderLock) {
            AnalysisServiceHolder holder = this.analysisServiceHolder;
            this.analysisServiceHolder = analysisServiceHolder;
            if (holder != null) {
                holder.close();
            }
        }
    }

    /**
     * Returns the new analysis holder for use. The acquired service holder should be released by calling release() method when
     * operations with the wrapped analyzer is completed.
     */
    public AnalysisServiceHolder acquireAnalysisServiceHolder() {
        synchronized (holderLock) {
            if (analysisServiceHolder == null) {
                throw new IllegalArgumentException("cannot acquire analysis service");
            }
            analysisServiceHolder.acquire();
            return analysisServiceHolder;
        }
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        if (analysisServiceHolder != null) {
            analysisServiceHolder.close();
            analysisServiceHolder = null;
        }
    }


    /**
     * Reference counting analysis service holder
     */
    public static class AnalysisServiceHolder implements Releasable {
        private final AtomicInteger userCount = new AtomicInteger(1);
        private final AnalysisService analysisService;

        public AnalysisServiceHolder(AnalysisService analysisService) {
            this.analysisService = analysisService;
        }

        public void acquire() {
            userCount.incrementAndGet();
        }

        public void release() {
            if (userCount.decrementAndGet() == 0) {
                analysisService.close();
            }
        }

        public boolean hasAnalyzer(String analyzerName) {
            return analysisService.analyzer(analyzerName) != null;
        }

        public TokenStream tokenStream(String analyzerName, String fieldName, String text) {
            Analyzer analyzer = analysisService.analyzer(analyzerName);
            if (analyzer == null) {
                throw new IllegalArgumentException("unknown analyzer [" + analyzerName + "]");
            }
            return analyzer.tokenStream(fieldName, text);
        }

        @Override
        public void close() {
            release();
        }
    }

    private static IndexSettings getNaIndexSettings(Settings settings) {
        IndexMetaData metaData = IndexMetaData.builder(IndexMetaData.INDEX_UUID_NA_VALUE).settings(settings).build();
        return new IndexSettings(metaData, Settings.EMPTY);
    }

    private static Settings getAnonymousSettings(Settings providerSetting) {
        return Settings.builder().put(providerSetting)
                // for _na_
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .build();
    }

}
