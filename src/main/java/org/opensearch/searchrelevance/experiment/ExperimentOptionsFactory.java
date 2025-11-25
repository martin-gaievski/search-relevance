/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.experiment;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Factory class for creating ExperimentOptions based on the experiment name and parameters
 */
public class ExperimentOptionsFactory {

    public static final String EMPTY_EXPERIMENT_OPTIONS = "EMPTY_EXPERIMENT_OPTIONS";
    public static final String HYBRID_SEARCH_EXPERIMENT_OPTIONS = "HYBRID_SEARCH_EXPERIMENT_OPTIONS";

    private static final Map<String, Function<Map<String, Object>, ExperimentOptions>> OPTIONS_BY_EXPERIMENT_NAME = Map.of(
        EMPTY_EXPERIMENT_OPTIONS,
        params -> new EmptyExperimentOptions(),
        HYBRID_SEARCH_EXPERIMENT_OPTIONS,
        ExperimentOptionsFactory::getExperimentOptionsForHybridSearch
    );

    /**
     * Creates an ExperimentOptions object based on the provided experiment name and parameters.
     *
     * @param experimentName The name of the experiment.
     * @param params The parameters for the experiment.
     * @return An ExperimentOptions object.
     * @throws IllegalArgumentException If the provided experiment name is not supported.
     */
    public static ExperimentOptions createExperimentOptions(final String experimentName, final Map<String, Object> params) {
        return Optional.ofNullable(OPTIONS_BY_EXPERIMENT_NAME.get(experimentName))
            .orElseThrow(() -> new IllegalArgumentException("provided experiment name is not supported"))
            .apply(params);
    }

    private static ExperimentOptionsForHybridSearch getExperimentOptionsForHybridSearch(Map<String, Object> params) {
        ExperimentOptionsForHybridSearch.ExperimentOptionsForHybridSearchBuilder builder = ExperimentOptionsForHybridSearch.builder();

        Set<String> normalizationTechniques = null;
        Set<String> combinationTechniques = null;

        if (params.containsKey("normalizationTechniques")) {
            normalizationTechniques = (Set<String>) params.get("normalizationTechniques");
            builder.normalizationTechniques(normalizationTechniques);
        }

        if (params.containsKey("combinationTechniques")) {
            combinationTechniques = (Set<String>) params.get("combinationTechniques");
            builder.combinationTechniques(combinationTechniques);
        }

        // Validate z_score compatibility
        if (normalizationTechniques != null && combinationTechniques != null) {
            if (normalizationTechniques.contains("z_score")) {
                // z_score requires arithmetic_mean to be available as a combination technique
                if (!combinationTechniques.contains("arithmetic_mean")) {
                    throw new IllegalArgumentException(
                        "z_score normalization technique requires arithmetic_mean to be included in combination techniques. "
                            + "Found combination techniques: "
                            + combinationTechniques
                    );
                }
            }
        }

        if (params.containsKey("weightsRange")) {
            Map<String, Object> weightsRangeMap = (Map<String, Object>) params.get("weightsRange");
            ExperimentOptionsForHybridSearch.WeightsRange.WeightsRangeBuilder weightsRangeBuilder =
                ExperimentOptionsForHybridSearch.WeightsRange.builder();

            if (weightsRangeMap.containsKey("rangeMin")) {
                weightsRangeBuilder.rangeMin(((Number) weightsRangeMap.get("rangeMin")).floatValue());
            }

            if (weightsRangeMap.containsKey("rangeMax")) {
                weightsRangeBuilder.rangeMax(((Number) weightsRangeMap.get("rangeMax")).floatValue());
            }

            if (weightsRangeMap.containsKey("increment")) {
                weightsRangeBuilder.increment(((Number) weightsRangeMap.get("increment")).floatValue());
            }

            builder.weightsRange(weightsRangeBuilder.build());
        }

        return builder.build();
    }

    /**
     * Creates a default set of experiment parameters for hybrid search.
     *
     * @return A map containing the default experiment parameters.
     */
    public static Map<String, Object> createDefaultExperimentParametersForHybridSearch() {
        return Map.of(
            "normalizationTechniques",
            Set.of("min_max", "l2", "z_score"),
            "combinationTechniques",
            Set.of("arithmetic_mean", "geometric_mean", "harmonic_mean", "rrf"),
            "weightsRange",
            Map.of("rangeMin", 0.0, "rangeMax", 1.0, "increment", 0.1)
        );
    }
}
