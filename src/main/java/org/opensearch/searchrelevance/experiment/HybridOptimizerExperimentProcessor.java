/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.experiment;

import static org.opensearch.searchrelevance.common.MetricsConstants.POINTWISE_FIELD_NAME_SEARCH_CONFIGURATION_ID;
import static org.opensearch.searchrelevance.experiment.ExperimentOptionsForHybridSearch.EXPERIMENT_OPTION_COMBINATION_TECHNIQUE;
import static org.opensearch.searchrelevance.experiment.ExperimentOptionsForHybridSearch.EXPERIMENT_OPTION_NORMALIZATION_TECHNIQUE;
import static org.opensearch.searchrelevance.experiment.ExperimentOptionsForHybridSearch.EXPERIMENT_OPTION_WEIGHTS_FOR_COMBINATION;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.searchrelevance.dao.JudgmentDao;
import org.opensearch.searchrelevance.executors.HybridSearchTaskManager;
import org.opensearch.searchrelevance.model.AsyncStatus;
import org.opensearch.searchrelevance.model.ExperimentType;
import org.opensearch.searchrelevance.model.ExperimentVariant;
import org.opensearch.searchrelevance.utils.TimeUtils;

import lombok.extern.log4j.Log4j2;

/**
 * Processor for handling HYBRID_OPTIMIZER experiments with asynchronous task management
 */
@Log4j2
public class HybridOptimizerExperimentProcessor {

    private final JudgmentDao judgmentDao;
    private final HybridSearchTaskManager taskManager;

    public HybridOptimizerExperimentProcessor(JudgmentDao judgmentDao, HybridSearchTaskManager taskManager) {
        this.judgmentDao = judgmentDao;
        this.taskManager = taskManager;
    }

    /**
     * Process hybrid optimizer experiment using task queue
     *
     * @param experimentId Experiment ID
     * @param queryText Query text to process
     * @param indexAndQueries Map of search configuration IDs to [index, query]
     * @param judgmentList List of judgment IDs
     * @param size Result size
     * @param hasFailure Failure flag
     * @param listener Listener to notify when processing is complete
     */
    public void processHybridOptimizerExperiment(
        String experimentId,
        String queryText,
        Map<String, List<String>> indexAndQueries,
        List<String> judgmentList,
        int size,
        AtomicBoolean hasFailure,
        ActionListener<Map<String, Object>> listener
    ) {
        // Create parameter combinations for hybrid search
        Map<String, Object> defaultParametersForHybridSearch = ExperimentOptionsFactory.createDefaultExperimentParametersForHybridSearch();
        ExperimentOptionsForHybridSearch experimentOptionForHybridSearch = (ExperimentOptionsForHybridSearch) ExperimentOptionsFactory
            .createExperimentOptions(ExperimentOptionsFactory.HYBRID_SEARCH_EXPERIMENT_OPTIONS, defaultParametersForHybridSearch);

        List<ExperimentVariantHybridSearchDTO> experimentVariantDTOs = experimentOptionForHybridSearch.getParameterCombinations(true);
        List<ExperimentVariant> experimentVariants = new ArrayList<>();

        log.info(
            "Starting hybrid optimizer experiment {} with {} parameter combinations for query: {}",
            experimentId,
            experimentVariantDTOs.size(),
            queryText
        );
        for (ExperimentVariantHybridSearchDTO experimentVariantDTO : experimentVariantDTOs) {
            Map<String, Object> parameters = new HashMap<>(
                Map.of(
                    EXPERIMENT_OPTION_NORMALIZATION_TECHNIQUE,
                    experimentVariantDTO.getNormalizationTechnique(),
                    EXPERIMENT_OPTION_COMBINATION_TECHNIQUE,
                    experimentVariantDTO.getCombinationTechnique(),
                    EXPERIMENT_OPTION_WEIGHTS_FOR_COMBINATION,
                    experimentVariantDTO.getQueryWeightsForCombination()
                )
            );
            String experimentVariantId = UUID.randomUUID().toString();

            // Create lightweight ExperimentVariant without storing it to index
            ExperimentVariant experimentVariant = new ExperimentVariant(
                experimentVariantId,
                TimeUtils.getTimestamp(),
                ExperimentType.HYBRID_OPTIMIZER,
                AsyncStatus.PROCESSING,
                experimentId,
                parameters,
                Map.of()
            );
            experimentVariants.add(experimentVariant);
        }

        log.info(
            "Experiment {}: Created {} experiment variants, proceeding to judgment processing",
            experimentId,
            experimentVariants.size()
        );

        // Process experiment variants for each search configuration
        Map<String, Object> hydratedResults = new ConcurrentHashMap<>();

        try {
            // Get document scores from judgments synchronously
            List<SearchResponse> judgmentResponses = new ArrayList<>();
            for (String judgmentId : judgmentList) {
                SearchResponse judgmentResponse = judgmentDao.getJudgmentSync(judgmentId);
                judgmentResponses.add(judgmentResponse);
            }

            Map<String, String> docIdToScores = processJudgments(queryText, judgmentResponses);
            log.info("Processing search configurations for query '{}' with {} document ratings", queryText, docIdToScores.size());

            // Process search configurations with task manager
            processSearchConfigurations(
                experimentId,
                queryText,
                indexAndQueries,
                judgmentList,
                size,
                experimentVariants,
                docIdToScores,
                hydratedResults,
                hasFailure,
                listener
            );
        } catch (Exception e) {
            if (hasFailure.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }
    }

    /**
     * Process judgments to extract document scores from SearchResponse objects (synchronous)
     */
    private Map<String, String> processJudgments(String queryText, List<SearchResponse> judgmentResponses) {
        log.info("Processing {} judgment responses for query: {}", judgmentResponses.size(), queryText);

        Map<String, String> docIdToScores = new HashMap<>();

        for (SearchResponse judgmentResponse : judgmentResponses) {
            try {
                if (judgmentResponse.getHits().getTotalHits().value() == 0) {
                    log.warn("No judgment found in response");
                } else {
                    Map<String, Object> sourceAsMap = judgmentResponse.getHits().getHits()[0].getSourceAsMap();
                    List<Map<String, Object>> judgmentRatings = (List<Map<String, Object>>) sourceAsMap.getOrDefault(
                        "judgmentRatings",
                        Collections.emptyList()
                    );

                    for (Map<String, Object> rating : judgmentRatings) {
                        if (queryText.equals(rating.get("query"))) {
                            List<Map<String, String>> docScoreRatings = (List<Map<String, String>>) rating.get("ratings");
                            if (docScoreRatings != null) {
                                docScoreRatings.forEach(
                                    docScoreRating -> docIdToScores.put(docScoreRating.get("docId"), docScoreRating.get("rating"))
                                );
                            }
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Failed to process judgment response: {}", e.getMessage());
            }
        }

        if (docIdToScores.isEmpty()) {
            log.warn("No ratings found for query: {} in any judgment responses", queryText);
        } else {
            log.info("Found {} document ratings for query: {}", docIdToScores.size(), queryText);
        }

        return docIdToScores;
    }

    /**
     * Process search configurations using task manager
     */
    private void processSearchConfigurations(
        String experimentId,
        String queryText,
        Map<String, List<String>> indexAndQueries,
        List<String> judgmentList,
        int size,
        List<ExperimentVariant> experimentVariants,
        Map<String, String> docIdToScores,
        Map<String, Object> hydratedResults,
        AtomicBoolean hasFailure,
        ActionListener<Map<String, Object>> finalListener
    ) {
        // Count for search configurations
        AtomicInteger pendingConfigurations = new AtomicInteger(indexAndQueries.size());
        List<Map<String, Object>> queryResults = Collections.synchronizedList(new ArrayList<>());

        // Process each search configuration
        for (Map.Entry<String, List<String>> entry : indexAndQueries.entrySet()) {
            String searchConfigId = entry.getKey();
            String index = entry.getValue().get(0);
            String query = entry.getValue().get(1);

            // Use task manager to process variants for this search config
            taskManager.scheduleTasks(
                experimentId,
                searchConfigId,
                index,
                query,
                queryText,
                size,
                experimentVariants,
                judgmentList,
                docIdToScores,
                hydratedResults,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Map<String, Object> results) {
                        try {
                            // Extract evaluation results and format them for the experiment
                            List<Map<String, Object>> evaluationResults = (List<Map<String, Object>>) results.get("evaluationResults");

                            synchronized (queryResults) {
                                // Create one result entry for this search configuration with all evaluation results
                                if (evaluationResults != null && !evaluationResults.isEmpty()) {
                                    Map<String, Object> searchConfigResult = new HashMap<>();
                                    searchConfigResult.put(POINTWISE_FIELD_NAME_SEARCH_CONFIGURATION_ID, searchConfigId);
                                    searchConfigResult.put("evaluationResults", new ArrayList<>(evaluationResults));
                                    // queryText will be added by handleQueryResults
                                    queryResults.add(searchConfigResult);
                                }

                                // Check if all search configurations for this query are complete
                                if (pendingConfigurations.decrementAndGet() == 0) {
                                    // All search configurations processed, return results for this query
                                    Map<String, Object> queryResponse = new HashMap<>();
                                    queryResponse.put("searchConfigurationResults", new ArrayList<>(queryResults));
                                    finalListener.onResponse(queryResponse);
                                }
                            }
                        } catch (Exception e) {
                            if (hasFailure.compareAndSet(false, true)) {
                                finalListener.onFailure(e);
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (hasFailure.compareAndSet(false, true)) {
                            finalListener.onFailure(e);
                        }
                    }
                },
                hasFailure
            );
        }
    }
}
