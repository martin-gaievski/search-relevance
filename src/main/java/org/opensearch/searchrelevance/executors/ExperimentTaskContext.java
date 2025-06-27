/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.executors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.core.action.ActionListener;
import org.opensearch.searchrelevance.dao.ExperimentVariantDao;
import org.opensearch.searchrelevance.model.ExperimentBatchStatus;
import org.opensearch.searchrelevance.model.ExperimentVariant;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Context for tracking tasks for a specific experiment
 */
@Log4j2
@Getter
public class ExperimentTaskContext {
    private final String experimentId;
    private final String searchConfigId;
    private final String queryText;
    private final int totalVariants;
    private final Map<String, Object> configToExperimentVariants;
    private final ActionListener<Map<String, Object>> finalListener;
    private final AtomicBoolean hasFailure;
    private final ExperimentVariantDao experimentVariantDao;
    
    private final AtomicInteger remainingVariants;
    private final AtomicInteger successfulVariants = new AtomicInteger(0);
    private final AtomicInteger failedVariants = new AtomicInteger(0);

    @Builder
    public ExperimentTaskContext(
        String experimentId,
        String searchConfigId,
        String queryText,
        int totalVariants,
        Map<String, Object> configToExperimentVariants,
        ActionListener<Map<String, Object>> finalListener,
        AtomicBoolean hasFailure,
        ExperimentVariantDao experimentVariantDao
    ) {
        this.experimentId = experimentId;
        this.searchConfigId = searchConfigId;
        this.queryText = queryText;
        this.totalVariants = totalVariants;
        this.configToExperimentVariants = configToExperimentVariants;
        this.finalListener = finalListener;
        this.hasFailure = hasFailure;
        this.experimentVariantDao = experimentVariantDao;
        this.remainingVariants = new AtomicInteger(totalVariants);
        
        log.info("TaskContext initialized for experiment {} with {} variants", experimentId, totalVariants);
    }

    /**
     * Write a variant individually using efficient refresh policy
     */
    public void scheduleVariantWrite(ExperimentVariant variant, String evaluationId, boolean isSuccess) {
        experimentVariantDao.putExperimentVariantEfficient(variant, ActionListener.wrap(response -> {
            log.debug("write successful for variant: {}", variant.getId());
            if (isSuccess) {
                synchronized (configToExperimentVariants) {
                    Map<String, Object> map = (Map<String, Object>) configToExperimentVariants.get(searchConfigId);
                    map.put(variant.getId(), evaluationId);
                }
            }
        }, error -> { 
            log.error("write failed for variant {}: {}", variant.getId(), error.getMessage()); 
        }));
    }

    /**
     * Mark a variant as successfully completed
     */
    public void completeVariantSuccess() {
        successfulVariants.incrementAndGet();
        completeVariant();
    }

    /**
     * Mark a variant as failed
     */
    public void completeVariantFailure() {
        failedVariants.incrementAndGet();
        completeVariant();
    }

    /**
     * Mark a variant as complete and check if all variants are done
     */
    private void completeVariant() {
        if (remainingVariants.decrementAndGet() == 0) {
            finishExperiment();
        }
    }

    /**
     * Finish the experiment and send final response
     */
    protected void finishExperiment() {
        // Create results even if all variants failed (non-fatal to experiment)
        Map<String, Object> transformedConfigToExperimentVariants = new HashMap<>();
        transformedConfigToExperimentVariants.put("searchConfigurationId", searchConfigId);

        List<Map<String, Object>> evaluationResults = formatEvaluationResults();
        transformedConfigToExperimentVariants.put("evaluationResults", evaluationResults);

        // Add failure summary
        Map<String, Object> summary = new HashMap<>();
        summary.put("totalVariants", totalVariants);
        summary.put("successfulVariants", successfulVariants.get());
        summary.put("failedVariants", failedVariants.get());
        transformedConfigToExperimentVariants.put("summary", summary);

        if (failedVariants.get() == totalVariants) {
            log.error(
                "All {} variants failed for search config {} in experiment {} with query '{}' - continuing experiment",
                totalVariants,
                searchConfigId,
                experimentId,
                queryText
            );
            transformedConfigToExperimentVariants.put("status", ExperimentBatchStatus.ALL_FAILED);
        } else if (failedVariants.get() > 0) {
            log.warn(
                "Partial failure for search config {} in experiment {} with query '{}': {}/{} variants succeeded",
                searchConfigId,
                experimentId,
                queryText,
                successfulVariants.get(),
                totalVariants
            );
            transformedConfigToExperimentVariants.put("status", ExperimentBatchStatus.PARTIAL_SUCCESS);
        } else {
            transformedConfigToExperimentVariants.put("status", ExperimentBatchStatus.SUCCESS);
        }

        // continue, don't fail the entire experiment for one search config
        finalListener.onResponse(transformedConfigToExperimentVariants);
    }

    /**
     * Format evaluation results for the final response
     */
    private List<Map<String, Object>> formatEvaluationResults() {
        List<Map<String, Object>> results = new java.util.ArrayList<>();
        Map<String, Object> configMap = (Map<String, Object>) configToExperimentVariants.get(searchConfigId);

        configMap.forEach((variantId, evalId) -> {
            Map<String, Object> result = new HashMap<>();
            result.put("evaluationId", evalId);
            result.put("experimentVariantId", variantId);
            results.add(result);
        });

        return results;
    }
}
