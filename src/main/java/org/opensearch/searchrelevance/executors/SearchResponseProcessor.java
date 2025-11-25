/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.executors;

import static org.opensearch.searchrelevance.common.MetricsConstants.PAIRWISE_FIELD_NAME_METRIC;
import static org.opensearch.searchrelevance.common.MetricsConstants.PAIRWISE_FIELD_NAME_VALUE;
import static org.opensearch.searchrelevance.metrics.EvaluationMetrics.calculateEvaluationMetrics;
import static org.opensearch.searchrelevance.metrics.calculator.Evaluation.METRICS_NORMALIZED_DISCOUNTED_CUMULATIVE_GAIN_AT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;
import org.opensearch.searchrelevance.dao.EvaluationResultDao;
import org.opensearch.searchrelevance.dao.ExperimentVariantDao;
import org.opensearch.searchrelevance.model.AsyncStatus;
import org.opensearch.searchrelevance.model.EvaluationResult;
import org.opensearch.searchrelevance.model.ExperimentType;
import org.opensearch.searchrelevance.model.ExperimentVariant;
import org.opensearch.searchrelevance.utils.TimeUtils;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Handles processing of search responses for experiment variants
 */
@Log4j2
@RequiredArgsConstructor
public class SearchResponseProcessor {
    private final EvaluationResultDao evaluationResultDao;
    private final ExperimentVariantDao experimentVariantDao;

    // Track best performing configurations per query
    private final ConcurrentHashMap<String, BestConfigurationTracker> bestConfigurationsPerQuery = new ConcurrentHashMap<>();
    // Track if we've already logged the best configuration for a query
    private final ConcurrentHashMap<String, Boolean> loggedQueries = new ConcurrentHashMap<>();
    // Track aggregated statistics for weight combinations per experiment
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> experimentWeightStats = new ConcurrentHashMap<>();
    // Track if we've logged aggregated stats for an experiment
    private final ConcurrentHashMap<String, Boolean> loggedExperimentStats = new ConcurrentHashMap<>();

    /**
     * Inner class to track best configuration for a query
     */
    private static class BestConfigurationTracker {
        double bestNdcg = -1.0;
        String bestConfigId;
        String bestVariantParams;
        String bestWeights;  // Track just the weights for easy aggregation
        String queryText;
        int variantsProcessed = 0;

        synchronized void updateIfBetter(double ndcg, String configId, String variantParams, String query) {
            variantsProcessed++;
            if (ndcg > bestNdcg) {
                bestNdcg = ndcg;
                bestConfigId = configId;
                bestVariantParams = variantParams;
                bestWeights = extractWeights(variantParams);
                queryText = query;
            }
        }

        synchronized void logBestConfiguration() {
            if (bestConfigId != null) {
                log.info("===== BEST CONFIGURATION FOR QUERY =====");
                log.info("Query: '{}'", queryText);
                log.info("Best NDCG@10: {}", bestNdcg);
                log.info("Best Search Configuration ID: {}", bestConfigId);
                log.info("Best Variant Parameters: {}", bestVariantParams);
                log.info("Best Weights: {}", bestWeights);
                log.info("Total variants evaluated: {}", variantsProcessed);
                log.info("=========================================");
            }
        }

        private String extractWeights(String variantParams) {
            if (variantParams == null) return "unknown";
            // Format appears to be: "combination_technique, normalization, weights"
            // Example: "arithmetic_mean, l2, 0.3;0.7"
            String[] parts = variantParams.split(",");
            if (parts.length >= 3) {
                String weights = parts[parts.length - 1].trim();
                // Convert semicolon to forward slash for better readability
                return weights.replace(";", "/");
            }
            return "unknown";
        }
    }

    /**
     * Process search response and create evaluation results
     */
    public void processSearchResponse(
        SearchResponse response,
        ExperimentVariant experimentVariant,
        String experimentId,
        String searchConfigId,
        String queryText,
        int size,
        List<String> judgmentIds,
        Map<String, String> docIdToScores,
        String evaluationId,
        ExperimentTaskContext taskContext
    ) {
        if (taskContext.getHasFailure().get()) return;

        try {
            if (response.getHits().getTotalHits().value() == 0) {
                handleNoHits(experimentVariant, experimentId, searchConfigId, evaluationId, taskContext);
                return;
            }

            SearchHit[] hits = response.getHits().getHits();
            List<String> docIds = Arrays.stream(hits).map(SearchHit::getId).collect(Collectors.toList());

            List<Map<String, Object>> metrics = calculateEvaluationMetrics(docIds, docIdToScores, size);

            // Extract NDCG value from metrics
            double ndcgValue = extractNdcgValue(metrics, size);

            // Track best configuration for this query
            if (experimentVariant.getType() == ExperimentType.HYBRID_OPTIMIZER && ndcgValue >= 0) {
                String queryKey = experimentId + "_" + queryText;
                BestConfigurationTracker tracker = bestConfigurationsPerQuery.computeIfAbsent(
                    queryKey,
                    k -> new BestConfigurationTracker()
                );

                String variantParams = experimentVariant.getTextualParameters();
                tracker.updateIfBetter(ndcgValue, searchConfigId, variantParams, queryText);
            }

            // Pass null for experiment variant parameters if not a hybrid experiment
            String experimentVariantParameters = experimentVariant.getType() == ExperimentType.HYBRID_OPTIMIZER
                ? experimentVariant.getTextualParameters()
                : null;

            EvaluationResult evaluationResult = new EvaluationResult(
                evaluationId,
                TimeUtils.getTimestamp(),
                searchConfigId,
                queryText,
                judgmentIds,
                docIds,
                metrics,
                experimentId,
                experimentVariant.getId(),
                experimentVariantParameters
            );

            evaluationResultDao.putEvaluationResultEfficient(
                evaluationResult,
                ActionListener.wrap(
                    success -> updateExperimentVariant(
                        experimentVariant,
                        experimentId,
                        searchConfigId,
                        evaluationId,
                        taskContext,
                        queryText
                    ),
                    error -> handleTaskFailure(experimentVariant, error, taskContext)
                )
            );
        } catch (Exception e) {
            handleTaskFailure(experimentVariant, e, taskContext);
        }
    }

    /**
     * Extract NDCG value from metrics list
     */
    private double extractNdcgValue(List<Map<String, Object>> metrics, int k) {
        String ndcgMetricName = METRICS_NORMALIZED_DISCOUNTED_CUMULATIVE_GAIN_AT + k;

        for (Map<String, Object> metric : metrics) {
            String metricName = (String) metric.get(PAIRWISE_FIELD_NAME_METRIC);
            if (ndcgMetricName.equals(metricName)) {
                Object value = metric.get(PAIRWISE_FIELD_NAME_VALUE);
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
            }
        }
        return -1.0; // Return -1 if NDCG not found
    }

    private void handleNoHits(
        ExperimentVariant experimentVariant,
        String experimentId,
        String searchConfigId,
        String evaluationId,
        ExperimentTaskContext taskContext
    ) {
        log.warn("No hits found for search config: {} and variant: {}", searchConfigId, experimentVariant.getId());

        ExperimentVariant noHitsVariant = new ExperimentVariant(
            experimentVariant.getId(),
            TimeUtils.getTimestamp(),
            experimentVariant.getType(),
            AsyncStatus.COMPLETED,
            experimentId,
            experimentVariant.getParameters(),
            Map.of("evaluationResultId", evaluationId, "details", "no search hits found")
        );

        experimentVariantDao.putExperimentVariantEfficient(noHitsVariant, ActionListener.wrap(success -> {
            log.debug("Persisted no-hits variant: {}", experimentVariant.getId());
            taskContext.completeVariantFailure();
        }, error -> handleTaskFailure(experimentVariant, error, taskContext)));
    }

    private void updateExperimentVariant(
        ExperimentVariant experimentVariant,
        String experimentId,
        String searchConfigId,
        String evaluationId,
        ExperimentTaskContext taskContext,
        String queryText
    ) {
        // Create variant directly with COMPLETED status
        ExperimentVariant completedVariant = new ExperimentVariant(
            experimentVariant.getId(),
            TimeUtils.getTimestamp(),
            experimentVariant.getType(),
            AsyncStatus.COMPLETED,
            experimentId,
            experimentVariant.getParameters(),
            Map.of("evaluationResultId", evaluationId)
        );

        taskContext.scheduleVariantWrite(completedVariant, evaluationId, true);

        log.debug("Scheduled write for completed experiment variant: {}", experimentVariant.getId());
        taskContext.completeVariantSuccess();

        // Check if this is the last variant for this query and log best configuration
        log.debug("logging best search configuration for query: {}", queryText);
        checkAndLogBestConfiguration(experimentId, taskContext, searchConfigId, queryText);
    }

    /**
     * Check if all variants are complete and log best configuration
     */
    private void checkAndLogBestConfiguration(
        String experimentId,
        ExperimentTaskContext taskContext,
        String searchConfigId,
        String queryText
    ) {
        // Log best configuration when task context indicates completion
        // Use a unique key per experiment and query to prevent duplicate logging
        String queryKey = experimentId + "_" + queryText;

        if (queryKey != null) {
            int completedVariants = taskContext.getSuccessfulVariants().get() + taskContext.getFailedVariants().get();
            if (completedVariants >= taskContext.getTotalVariants() - 1) {
                // Only log if we haven't already logged for this query
                if (loggedQueries.putIfAbsent(queryKey, Boolean.TRUE) == null) {
                    BestConfigurationTracker tracker = bestConfigurationsPerQuery.get(queryKey);
                    if (tracker != null) {
                        tracker.logBestConfiguration();

                        // Track aggregated weight statistics
                        trackWeightStatistics(experimentId, tracker.bestWeights);
                    }
                }

                // Check if this is the last query for the experiment and log aggregated stats
                checkAndLogAggregatedStats(experimentId);
            }
        }
    }

    /**
     * Track weight statistics for aggregation
     */
    private void trackWeightStatistics(String experimentId, String weights) {
        if (weights != null && !weights.equals("unknown")) {
            ConcurrentHashMap<String, Integer> weightStats = experimentWeightStats.computeIfAbsent(
                experimentId,
                k -> new ConcurrentHashMap<>()
            );
            weightStats.merge(weights, 1, Integer::sum);
        }
    }

    /**
     * Check and log aggregated statistics for the experiment
     */
    private void checkAndLogAggregatedStats(String experimentId) {
        // Count how many queries we've processed for this experiment
        long queriesForExperiment = bestConfigurationsPerQuery.keySet().stream().filter(key -> key.startsWith(experimentId)).count();

        long loggedQueriesForExperiment = loggedQueries.keySet().stream().filter(key -> key.startsWith(experimentId)).count();

        // Debug logging to understand what's happening with multi-query experiments
        log.debug(
            "Checking aggregated stats for experiment {}: queries tracked={}, queries logged={}",
            experimentId,
            queriesForExperiment,
            loggedQueriesForExperiment
        );

        // If all queries for this experiment have been logged, show aggregated stats
        if (queriesForExperiment > 0 && queriesForExperiment == loggedQueriesForExperiment) {
            if (loggedExperimentStats.putIfAbsent(experimentId, Boolean.TRUE) == null) {
                log.info("All {} queries complete for experiment {}, logging aggregated statistics", queriesForExperiment, experimentId);
                logAggregatedStatistics(experimentId);
            }
        } else {
            log.debug("Not logging aggregated stats yet: {} of {} queries complete", loggedQueriesForExperiment, queriesForExperiment);
        }
    }

    /**
     * Log aggregated statistics for an experiment
     */
    private void logAggregatedStatistics(String experimentId) {
        ConcurrentHashMap<String, Integer> weightStats = experimentWeightStats.get(experimentId);
        if (weightStats != null && !weightStats.isEmpty()) {
            log.info("===== AGGREGATED WEIGHT STATISTICS FOR EXPERIMENT =====");
            log.info("Experiment ID: {}", experimentId);
            log.info("Distribution of best weights across queries:");

            // Sort by weight values for better readability
            weightStats.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                String weights = entry.getKey();
                int count = entry.getValue();
                log.info("  Weights {} -> {} queries", weights, count);
            });

            int totalQueries = weightStats.values().stream().mapToInt(Integer::intValue).sum();
            log.info("Total queries analyzed: {}", totalQueries);

            // Find most common weight combination
            Map.Entry<String, Integer> mostCommon = weightStats.entrySet().stream().max(Map.Entry.comparingByValue()).orElse(null);

            if (mostCommon != null) {
                double percentage = (mostCommon.getValue() * 100.0) / totalQueries;
                log.info(
                    "Most common weight combination: {} ({} queries, {}%)",
                    mostCommon.getKey(),
                    mostCommon.getValue(),
                    String.format("%.1f", percentage)
                );
            }

            // Calculate and log distribution metrics
            logDistributionMetrics(weightStats, totalQueries);

            // Calculate and log average of best NDCGs
            double avgBestNdcg = calculateAverageBestNDCG(experimentId);
            if (avgBestNdcg >= 0) {
                log.info("Average of best NDCG@10 across all queries: {}", String.format("%.4f", avgBestNdcg));
            }

            log.info("========================================================");
        }
    }

    /**
     * Calculate and log distribution metrics for weight combinations
     */
    private void logDistributionMetrics(ConcurrentHashMap<String, Integer> weightStats, int totalQueries) {
        // Calculate entropy (normalized)
        double entropy = calculateNormalizedEntropy(weightStats, totalQueries);

        // Calculate Gini coefficient
        double gini = calculateGiniCoefficient(weightStats, totalQueries);

        // Calculate coefficient of variation
        double cv = calculateCoefficientOfVariation(weightStats);

        log.info("===== DISTRIBUTION METRICS =====");
        log.info("Normalized Entropy: {}", String.format("%.4f", entropy));
        log.info("  (0 = concentrated on single weight, 1 = perfectly uniform)");
        log.info("Gini Coefficient: {}", String.format("%.4f", gini));
        log.info("  (0 = perfect equality, 1 = perfect inequality)");
        log.info("Coefficient of Variation: {}", String.format("%.4f", cv));
        log.info("  (< 0.5 = low variation, 0.5-1.0 = moderate, > 1.0 = high variation)");
    }

    /**
     * Calculate normalized entropy to measure distribution uniformity
     * Returns value between 0 (all queries on one weight) and 1 (perfectly uniform)
     */
    private double calculateNormalizedEntropy(Map<String, Integer> distribution, int total) {
        if (distribution.size() <= 1) return 0.0;

        double entropy = 0.0;
        for (int count : distribution.values()) {
            if (count > 0) {
                double probability = (double) count / total;
                entropy -= probability * Math.log(probability) / Math.log(2);
            }
        }

        // Normalize by maximum possible entropy (log2 of number of categories)
        double maxEntropy = Math.log(distribution.size()) / Math.log(2);
        return maxEntropy > 0 ? entropy / maxEntropy : 0.0;
    }

    /**
     * Calculate Gini coefficient to measure inequality in distribution
     * Returns value between 0 (perfect equality) and 1 (perfect inequality)
     */
    private double calculateGiniCoefficient(Map<String, Integer> distribution, int total) {
        List<Integer> values = new ArrayList<>(distribution.values());
        values.sort(Integer::compareTo);

        double cumulativeSum = 0.0;
        double giniSum = 0.0;

        for (int i = 0; i < values.size(); i++) {
            cumulativeSum += values.get(i);
            giniSum += cumulativeSum;
        }

        if (total == 0 || values.size() == 0) return 0.0;

        double equalShare = (double) total / values.size();
        double maxGiniSum = equalShare * values.size() * (values.size() + 1) / 2;

        return 1.0 - (2.0 * giniSum) / (values.size() * total);
    }

    /**
     * Calculate coefficient of variation (relative standard deviation)
     * Measures relative variability of the distribution
     */
    private double calculateCoefficientOfVariation(Map<String, Integer> distribution) {
        if (distribution.isEmpty()) return 0.0;

        // Calculate mean
        double sum = distribution.values().stream().mapToInt(Integer::intValue).sum();
        double mean = sum / distribution.size();

        // Calculate standard deviation
        double variance = 0.0;
        for (int value : distribution.values()) {
            variance += Math.pow(value - mean, 2);
        }
        variance /= distribution.size();
        double stdDev = Math.sqrt(variance);

        // Return coefficient of variation
        return mean > 0 ? stdDev / mean : 0.0;
    }

    /**
     * Calculate average of best NDCG values for all queries in an experiment
     */
    private double calculateAverageBestNDCG(String experimentId) {
        double sumBestNdcg = 0.0;
        int queryCount = 0;

        for (Map.Entry<String, BestConfigurationTracker> entry : bestConfigurationsPerQuery.entrySet()) {
            if (entry.getKey().startsWith(experimentId)) {
                BestConfigurationTracker tracker = entry.getValue();
                if (tracker.bestNdcg >= 0) {
                    sumBestNdcg += tracker.bestNdcg;
                    queryCount++;
                }
            }
        }

        if (queryCount > 0) {
            double avgNdcg = sumBestNdcg / queryCount;
            log.debug("Calculated average best NDCG: sum={}, count={}, avg={}", sumBestNdcg, queryCount, avgNdcg);
            return avgNdcg;
        }

        return -1.0; // Return -1 if no queries found
    }

    private void handleTaskFailure(ExperimentVariant experimentVariant, Exception e, ExperimentTaskContext taskContext) {
        log.error("Variant failure for {}: {}", experimentVariant.getId(), e.getMessage());
        taskContext.completeVariantFailure();
    }

    /**
     * Handle search failure
     */
    public void handleSearchFailure(
        Exception e,
        ExperimentVariant experimentVariant,
        String experimentId,
        String evaluationId,
        ExperimentTaskContext taskContext
    ) {
        ExperimentVariant experimentVariantResult = new ExperimentVariant(
            experimentVariant.getId(),
            TimeUtils.getTimestamp(),
            experimentVariant.getType(),
            AsyncStatus.ERROR,
            experimentId,
            experimentVariant.getParameters(),
            Map.of("evaluationResultId", evaluationId, "error", e.getMessage())
        );

        experimentVariantDao.putExperimentVariantEfficient(experimentVariantResult, ActionListener.wrap(success -> {
            log.error("Error executing variant {}: {}", experimentVariant.getId(), e.getMessage());
            taskContext.completeVariantFailure();
        }, error -> {
            log.error("Failed to persist error status for variant {}: {}", experimentVariant.getId(), error.getMessage());
            taskContext.completeVariantFailure();
        }));
    }
}
