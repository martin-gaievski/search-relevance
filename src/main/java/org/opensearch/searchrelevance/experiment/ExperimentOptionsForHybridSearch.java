/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.experiment;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
/**
 * Experiment options for hybrid search
 */
public class ExperimentOptionsForHybridSearch implements ExperimentOptions {
    private Set<String> normalizationTechniques;
    private Set<String> combinationTechniques;
    private WeightsRange weightsRange;

    public static final String EXPERIMENT_OPTION_NORMALIZATION_TECHNIQUE = "normalization";
    public static final String EXPERIMENT_OPTION_COMBINATION_TECHNIQUE = "combination";
    public static final String EXPERIMENT_OPTION_WEIGHTS_FOR_COMBINATION = "weights";

    @Data
    @Builder
    static class WeightsRange {
        private float rangeMin;
        private float rangeMax;
        private float increment;
    }

    public List<ExperimentVariantHybridSearchDTO> getParameterCombinations(boolean includeWeights) {
        List<ExperimentVariantHybridSearchDTO> allPossibleParameterCombinations = new ArrayList<>();

        // Handle RRF separately - it doesn't use normalization
        if (combinationTechniques.contains("rrf")) {
            if (includeWeights) {
                // use integer-based approach to avoid floating-point precision issues
                float min = weightsRange.getRangeMin();
                float max = weightsRange.getRangeMax();
                float increment = weightsRange.getIncrement();

                // calculate number of steps to ensure we include all values including the max
                int steps = Math.round((max - min) / increment) + 1;

                for (int i = 0; i < steps; i++) {
                    // calculate weight, ensuring the last step is exactly the max value
                    float queryWeightForCombination;
                    if (i == steps - 1) {
                        queryWeightForCombination = max;
                    } else {
                        queryWeightForCombination = min + (i * increment);
                    }

                    allPossibleParameterCombinations.add(
                        ExperimentVariantHybridSearchDTO.builder()
                            .normalizationTechnique(null)  // No normalization for RRF
                            .combinationTechnique("rrf")
                            .queryWeightsForCombination(new float[] { queryWeightForCombination, 1.0f - queryWeightForCombination })
                            .build()
                    );
                }
            } else {
                allPossibleParameterCombinations.add(
                    ExperimentVariantHybridSearchDTO.builder()
                        .normalizationTechnique(null)  // No normalization for RRF
                        .combinationTechnique("rrf")
                        .queryWeightsForCombination(new float[] { 0.5f, 0.5f })
                        .build()
                );
            }
        }

        // Handle regular normalization + combination techniques
        for (String normalizationTechnique : normalizationTechniques) {
            for (String combinationTechnique : combinationTechniques) {
                // Skip RRF as it's handled separately
                if ("rrf".equals(combinationTechnique)) {
                    continue;
                }

                // Validate z_score compatibility - it can only be combined with arithmetic_mean
                if ("z_score".equals(normalizationTechnique) && !"arithmetic_mean".equals(combinationTechnique)) {
                    continue; // Skip invalid combinations
                }

                if (includeWeights) {
                    // use integer-based approach to avoid floating-point precision issues
                    float min = weightsRange.getRangeMin();
                    float max = weightsRange.getRangeMax();
                    float increment = weightsRange.getIncrement();

                    // calculate number of steps to ensure we include all values including the max
                    int steps = Math.round((max - min) / increment) + 1;

                    for (int i = 0; i < steps; i++) {
                        // calculate weight, ensuring the last step is exactly the max value
                        float queryWeightForCombination;
                        if (i == steps - 1) {
                            queryWeightForCombination = max;
                        } else {
                            queryWeightForCombination = min + (i * increment);
                        }

                        allPossibleParameterCombinations.add(
                            ExperimentVariantHybridSearchDTO.builder()
                                .normalizationTechnique(normalizationTechnique)
                                .combinationTechnique(combinationTechnique)
                                .queryWeightsForCombination(new float[] { queryWeightForCombination, 1.0f - queryWeightForCombination })
                                .build()
                        );
                    }
                } else {
                    allPossibleParameterCombinations.add(
                        ExperimentVariantHybridSearchDTO.builder()
                            .normalizationTechnique(normalizationTechnique)
                            .combinationTechnique(combinationTechnique)
                            .queryWeightsForCombination(new float[] { 0.5f, 0.5f })
                            .build()
                    );
                }
            }
        }
        return allPossibleParameterCombinations;
    }
}
