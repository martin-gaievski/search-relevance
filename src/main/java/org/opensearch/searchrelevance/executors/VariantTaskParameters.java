/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.executors;

import java.util.List;
import java.util.Map;

import org.opensearch.searchrelevance.model.ExperimentVariant;

import lombok.Builder;
import lombok.Getter;

/**
 * Parameters for scheduling a variant task
 */
@Getter
@Builder
public class VariantTaskParameters {
    private final String experimentId;
    private final String searchConfigId;
    private final String index;
    private final String query;
    private final String queryText;
    private final int size;
    private final ExperimentVariant experimentVariant;
    private final List<String> judgmentIds;
    private final Map<String, String> docIdToScores;
    private final ExperimentTaskContext taskContext;
}
