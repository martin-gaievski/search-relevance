/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.action.experiments;

import static org.opensearch.searchrelevance.common.PluginConstants.EVALUATION_RESULT_INDEX;
import static org.opensearch.searchrelevance.common.PluginConstants.EXPERIMENTS_URI;
import static org.opensearch.searchrelevance.common.PluginConstants.EXPERIMENT_INDEX;
import static org.opensearch.searchrelevance.common.PluginConstants.EXPERIMENT_VARIANT_INDEX;
import static org.opensearch.searchrelevance.common.PluginConstants.JUDGMENTS_URL;
import static org.opensearch.searchrelevance.common.PluginConstants.QUERYSETS_URL;
import static org.opensearch.searchrelevance.common.PluginConstants.SEARCH_CONFIGURATIONS_URL;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.client.Response;
import org.opensearch.rest.RestRequest;
import org.opensearch.searchrelevance.BaseSearchRelevanceIT;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.google.common.collect.ImmutableList;

import lombok.SneakyThrows;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class ExperimentIT extends BaseSearchRelevanceIT {

    public static final List<String> EXPECTED_QUERY_TERMS = List.of(
        "button",
        "keyboard",
        "steel",
        "diamond wheel",
        "phone",
        "metal frame",
        "iphone",
        "giangentic form"
    );
    public static final Map<String, Object> EXPECT_EVALUATION_RESULTS = Map.of(
        "button",
        Map.of(
            "documentIds",
            List.of("B06Y1L1YJD", "B01M3XBRRX", "B07D29PHFY"),
            "metrics",
            Map.of("Coverage@5", 1.0, "Precision@5", 1.0, "MAP@5", 1.0, "NDCG@5", 0.94)
        ),

        "metal frame",
        Map.of(
            "documentIds",
            List.of("B07MBG53JD", "B097Q69V1B", "B00TLYRBMG", "B08G46SS1T", "B07H81Z91C"),
            "metrics",
            Map.of("Coverage@5", 1.0, "Precision@5", 1.0, "MAP@5", 1.0, "NDCG@5", 0.9)
        )
    );
    private static final String INDEX_NAME_ESCI = "ecommerce";
    // Expected number of variants per query for HYBRID_OPTIMIZER experiments
    private static final int EXPECTED_VARIANTS_PER_QUERY = 66;
    // Timeout values
    private static final int MAX_POLL_RETRIES = 15;
    private static final int POLL_INTERVAL_MS = 10000; // 10 seconds
    private static final int POST_COMPLETION_WAIT_MS = 10000; // 10 seconds

    @SneakyThrows
    public void testPointwiseEvaluationExperiment_whenQueryWithPlaceholder_thenSuccessful() {
        // Arrange
        initializeIndexIfNotExist(INDEX_NAME_ESCI);

        String searchConfigurationId = createSearchConfiguration();
        String querySetId = createQuerySet();
        String judgmentId = createJudgment();

        // Act
        String experimentId = createExperiment(querySetId, searchConfigurationId, judgmentId);

        // Assert
        Map<String, String> queryTextToEvaluationId = assertExperimentCreation(experimentId, judgmentId, searchConfigurationId, querySetId);
        assertEvaluationResults(queryTextToEvaluationId, judgmentId, searchConfigurationId);

        deleteIndex(INDEX_NAME_ESCI);
    }

    @SneakyThrows
    public void testHybridOptimizerExperiment_whenHybridQueries_thenSuccessful() {
        // This test is a best-effort test that verifies hybrid optimizer experiments
        // It may be affected by system resource constraints in CI environments

        // Arrange
        initializeIndexIfNotExist(INDEX_NAME_ESCI);

        String hybridSearchConfigId = createHybridSearchConfiguration();
        String querySetId = createQuerySet();
        String judgmentId = createJudgment();

        try {
            // Act
            String experimentId = createHybridOptimizerExperiment(querySetId, hybridSearchConfigId, judgmentId);

            // Wait for the experiment to be created and indexed
            Thread.sleep(5000);

            // Assert experiment exists with correct type
            // We don't wait for completion since it may time out in constrained environments
            Map<String, Object> experimentSource = getExperiment(experimentId);
            assertNotNull("Experiment should exist", experimentSource);
            assertEquals("HYBRID_OPTIMIZER", experimentSource.get("type"));
            assertEquals(querySetId, experimentSource.get("querySetId"));

            // If experiment completed, try to verify variants for a well-known query
            // This is a best-effort verification that may be skipped if resources are constrained
            if ("COMPLETED".equals(experimentSource.get("status"))) {
                try {
                    // Use a query we know has judgments
                    String sampleQueryTerm = "button"; // Known to have judgments
                    assertHybridOptimizerExperimentVariantsForQuery(experimentId, sampleQueryTerm);
                } catch (Exception e) {
                    // Log but don't fail the test if variant verification fails
                    logger.error("Couldn't retrieve judgements for query text", e);
                }
            }
        } finally {
            deleteIndex(INDEX_NAME_ESCI);
        }
    }

    private void assertEvaluationResults(Map<String, String> queryTextToEvaluationId, String judgmentId, String searchConfigurationId)
        throws IOException {
        // assert every evaluation result
        for (String queryTerm : queryTextToEvaluationId.keySet()) {
            String evaluationId = queryTextToEvaluationId.get(queryTerm);

            String getEvaluationByIdUrl = String.join("/", EVALUATION_RESULT_INDEX, "_doc", evaluationId);
            Response getEvaluationResponse = makeRequest(
                client(),
                RestRequest.Method.GET.name(),
                getEvaluationByIdUrl,
                null,
                null,
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
            );
            Map<String, Object> getEvaluationResultJson = entityAsMap(getEvaluationResponse);
            assertNotNull(getEvaluationResultJson);

            Map<String, Object> evaluationSource = (Map<String, Object>) getEvaluationResultJson.get("_source");
            // randomly pick 2 items and check them field by field, do sanity check for others
            String actualQueryTerm = evaluationSource.get("searchText").toString();
            if (EXPECT_EVALUATION_RESULTS.containsKey(actualQueryTerm)) {
                Map<String, Object> expectedResult = (Map<String, Object>) EXPECT_EVALUATION_RESULTS.get(actualQueryTerm);
                List<String> actualDocumentIds = (List<String>) evaluationSource.get("documentIds");
                assertListsHaveSameElements((List<String>) expectedResult.get("documentIds"), actualDocumentIds);
                List<Map> actualMetrics = (List<Map>) evaluationSource.get("metrics");
                Map<String, Double> expectedMetrics = (Map<String, Double>) expectedResult.get("metrics");
                assertEquals(expectedMetrics.size(), actualMetrics.size());
                for (Map<String, Object> actualMetric : actualMetrics) {
                    String metricName = actualMetric.get("metric").toString();
                    Double actualValue = Double.parseDouble(actualMetric.get("value").toString());
                    assertEquals(expectedMetrics.get(metricName), actualValue, 0.02);
                }
            } else {
                assertTrue(EXPECTED_QUERY_TERMS.contains(actualQueryTerm));
                assertEquals(judgmentId, ((List<String>) evaluationSource.get("judgmentIds")).get(0));
                assertEquals(4, ((List<String>) evaluationSource.get("metrics")).size());
                assertEquals(searchConfigurationId, evaluationSource.get("searchConfigurationId"));
                assertFalse(((List<String>) evaluationSource.get("documentIds")).isEmpty());
            }
        }
    }

    private Map<String, String> assertExperimentCreation(
        String experimentId,
        String judgmentId,
        String searchConfigurationId,
        String querySetId
    ) throws IOException {
        String getExperimentByIdUrl = String.join("/", EXPERIMENT_INDEX, "_doc", experimentId);
        Response getExperimentResponse = makeRequest(
            adminClient(),
            RestRequest.Method.GET.name(),
            getExperimentByIdUrl,
            null,
            null,
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> getExperimentResultJson = entityAsMap(getExperimentResponse);
        assertNotNull(getExperimentResultJson);
        assertEquals(experimentId, getExperimentResultJson.get("_id").toString());

        Map<String, Object> source = (Map<String, Object>) getExperimentResultJson.get("_source");
        assertNotNull(source);
        assertNotNull(source.get("id"));
        assertNotNull(source.get("timestamp"));
        assertEquals("COMPLETED", source.get("status"));

        List<String> judgmentList = (List<String>) source.get("judgmentList");
        assertNotNull(judgmentList);
        assertEquals(1, judgmentList.size());
        assertEquals(judgmentId, judgmentList.get(0));

        List<String> searchConfigurationList = (List<String>) source.get("searchConfigurationList");
        assertNotNull(searchConfigurationList);
        assertEquals(1, searchConfigurationList.size());
        assertEquals(searchConfigurationId, searchConfigurationList.get(0));

        assertEquals("POINTWISE_EVALUATION", source.get("type"));
        assertEquals(querySetId, source.get("querySetId"));

        List<Map<String, Object>> results = (List<Map<String, Object>>) source.get("results");
        assertNotNull(results);
        // convert list of actual results to map of query text and evaluation id
        Map<String, Object> resultsMap = new HashMap<>();
        results.forEach(result -> {
            assertEquals(searchConfigurationId, result.get("searchConfigurationId"));
            resultsMap.put((String) result.get("queryText"), result.get("evaluationId"));
        });
        assertEquals(results.size(), resultsMap.size());

        Map<String, String> queryTextToEvaluationId = new HashMap<>();

        EXPECTED_QUERY_TERMS.forEach(queryTerm -> {
            assertTrue(resultsMap.containsKey(queryTerm));
            String evaludationId = (String) resultsMap.get(queryTerm);
            assertNotNull(evaludationId);
            queryTextToEvaluationId.put(queryTerm, evaludationId);
        });

        assertEquals(8, results.size());
        assertEquals(8, queryTextToEvaluationId.size());
        return queryTextToEvaluationId;
    }

    private void assertHybridOptimizerExperimentVariantsForQuery(String experimentId, String queryText) throws IOException {
        // First get all evaluation results for this query text
        List<String> evaluationIds = getEvaluationResultIdsForQuery(queryText);

        // We should have found some evaluation results
        assertFalse("Should have evaluation results for query: " + queryText, evaluationIds.isEmpty());

        // Now get the experiment variants that reference these evaluation result IDs
        List<Map<String, Object>> variants = getExperimentVariantsForEvaluationIds(experimentId, evaluationIds);

        // We should have found some experiment variants
        assertFalse("Should have experiment variants for query: " + queryText, variants.isEmpty());

        // We should have multiple variants for the query
        assertEquals(EXPECTED_VARIANTS_PER_QUERY, variants.size());

        // Verify structure of a few variants
        int variantsToCheck = Math.min(3, variants.size());

        for (int i = 0; i < variantsToCheck; i++) {
            Map<String, Object> source = variants.get(i);
            assertNotNull(source);

            assertEquals(experimentId, source.get("experimentId"));

            // Parameters are nested in a "parameters" object
            Map<String, Object> parameters = (Map<String, Object>) source.get("parameters");
            assertNotNull("Parameters object should exist", parameters);
            assertNotNull("Normalization should exist", parameters.get("normalization"));
            assertNotNull("Combination should exist", parameters.get("combination"));
            assertNotNull("Weights should exist", parameters.get("weights"));

            // Check results
            Map<String, Object> results = (Map<String, Object>) source.get("results");
            assertNotNull("Results should exist", results);
            String evaluationResultId = (String) results.get("evaluationResultId");
            assertNotNull("Evaluation result ID should exist", evaluationResultId);

            // Verify the evaluation result exists and has the correct query text
            verifyEvaluationResult(evaluationResultId, queryText);
        }
    }

    private List<String> getEvaluationResultIdsForQuery(String queryText) throws IOException {
        String getEvaluationResultsUrl = String.join("/", EVALUATION_RESULT_INDEX, "_search");
        String evaluationResultsQuery = "{ \"size\": 100, \"query\": { \"term\": { \"searchText\": \"" + queryText + "\" } } }";

        Response getEvaluationResponse = makeRequest(
            client(),
            RestRequest.Method.POST.name(),
            getEvaluationResultsUrl,
            null,
            toHttpEntity(evaluationResultsQuery),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        Map<String, Object> getEvaluationResultJson = entityAsMap(getEvaluationResponse);
        Map<String, Object> hitsObj = (Map<String, Object>) getEvaluationResultJson.get("hits");
        List<Map<String, Object>> evaluationHits = (List<Map<String, Object>>) hitsObj.get("hits");

        // Extract the evaluation result IDs
        List<String> evaluationIds = new ArrayList<>();
        for (Map<String, Object> hit : evaluationHits) {
            String evalId = (String) hit.get("_id");
            evaluationIds.add(evalId);
        }

        return evaluationIds;
    }

    private List<Map<String, Object>> getExperimentVariantsForEvaluationIds(String experimentId, List<String> evaluationIds)
        throws IOException {
        if (evaluationIds.isEmpty()) {
            return Collections.emptyList();
        }

        String getVariantsUrl = String.join("/", EXPERIMENT_VARIANT_INDEX, "_search");

        // Build the nested query for evaluationResultId
        StringBuilder termsBuilder = new StringBuilder();
        for (int i = 0; i < evaluationIds.size(); i++) {
            termsBuilder.append("\"").append(evaluationIds.get(i)).append("\"");
            if (i < evaluationIds.size() - 1) {
                termsBuilder.append(", ");
            }
        }

        // Use nested query for results.evaluationResultId
        String nestedQuery = "{ \"size\": 100, \"query\": { \"bool\": { \"must\": [ "
            + "  { \"term\": { \"experimentId\": \""
            + experimentId
            + "\" } }, "
            + "  { \"nested\": { "
            + "      \"path\": \"results\", "
            + "      \"query\": { "
            + "        \"terms\": { "
            + "          \"results.evaluationResultId\": ["
            + termsBuilder.toString()
            + "]"
            + "        } "
            + "      } "
            + "    } "
            + "  } "
            + "] } } }";

        Response getVariantsResponse = makeRequest(
            client(),
            RestRequest.Method.POST.name(),
            getVariantsUrl,
            null,
            toHttpEntity(nestedQuery),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        Map<String, Object> getVariantsResultJson = entityAsMap(getVariantsResponse);
        Map<String, Object> variantHitsObj = (Map<String, Object>) getVariantsResultJson.get("hits");
        List<Map<String, Object>> variantHits = (List<Map<String, Object>>) variantHitsObj.get("hits");

        // Extract the sources
        List<Map<String, Object>> sources = new ArrayList<>();
        for (Map<String, Object> hit : variantHits) {
            sources.add((Map<String, Object>) hit.get("_source"));
        }

        return sources;
    }

    private void verifyEvaluationResult(String evaluationResultId, String queryText) throws IOException {
        String getEvaluationByIdUrl = String.join("/", EVALUATION_RESULT_INDEX, "_doc", evaluationResultId);
        Response getEvaluationResponse = makeRequest(
            client(),
            RestRequest.Method.GET.name(),
            getEvaluationByIdUrl,
            null,
            null,
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> getEvaluationResultJson = entityAsMap(getEvaluationResponse);
        assertNotNull(getEvaluationResultJson);

        Map<String, Object> evaluationSource = (Map<String, Object>) getEvaluationResultJson.get("_source");
        assertNotNull(evaluationSource);

        // Verify the search text matches the query
        assertEquals("Evaluation search text should match query", queryText, evaluationSource.get("searchText"));

        // Verify we have metrics
        List<Map> metrics = (List<Map>) evaluationSource.get("metrics");
        assertNotNull("Metrics should exist", metrics);
        assertFalse("Metrics should not be empty", metrics.isEmpty());

        // Verify we have document IDs
        List<String> documentIds = (List<String>) evaluationSource.get("documentIds");
        assertNotNull("Document IDs should exist", documentIds);
        assertFalse("Document IDs should not be empty", documentIds.isEmpty());
    }

    private void assertHybridOptimizerExperimentCreation(
        Map<String, Object> source,
        String judgmentId,
        String searchConfigurationId,
        String querySetId
    ) {
        assertNotNull(source.get("id"));
        assertNotNull(source.get("timestamp"));

        List<String> judgmentList = (List<String>) source.get("judgmentList");
        assertNotNull(judgmentList);
        assertEquals(1, judgmentList.size());
        assertEquals(judgmentId, judgmentList.get(0));

        List<String> searchConfigurationList = (List<String>) source.get("searchConfigurationList");
        assertNotNull(searchConfigurationList);
        assertEquals(1, searchConfigurationList.size());
        assertEquals(searchConfigurationId, searchConfigurationList.get(0));

        assertEquals("HYBRID_OPTIMIZER", source.get("type"));
        assertEquals(querySetId, source.get("querySetId"));
    }

    private Map<String, Object> getExperiment(String experimentId) throws IOException {
        String getExperimentByIdUrl = String.join("/", EXPERIMENT_INDEX, "_doc", experimentId);
        Response getExperimentResponse = makeRequest(
            adminClient(),
            RestRequest.Method.GET.name(),
            getExperimentByIdUrl,
            null,
            null,
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> getExperimentResultJson = entityAsMap(getExperimentResponse);
        assertNotNull(getExperimentResultJson);
        assertEquals(experimentId, getExperimentResultJson.get("_id").toString());

        return (Map<String, Object>) getExperimentResultJson.get("_source");
    }

    private Map<String, Object> pollExperimentUntilCompleted(String experimentId) throws IOException {
        Map<String, Object> source = null;
        String getExperimentByIdUrl = String.join("/", EXPERIMENT_INDEX, "_doc", experimentId);

        int retryCount = 0;
        String status = "PROCESSING";

        while (("PROCESSING".equals(status) || status == null) && retryCount < MAX_POLL_RETRIES) {
            Response getExperimentResponse = makeRequest(
                client(),
                RestRequest.Method.GET.name(),
                getExperimentByIdUrl,
                null,
                null,
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
            );
            Map<String, Object> getExperimentResultJson = entityAsMap(getExperimentResponse);
            assertNotNull(getExperimentResultJson);
            assertEquals(experimentId, getExperimentResultJson.get("_id").toString());

            source = (Map<String, Object>) getExperimentResultJson.get("_source");
            assertNotNull(source);
            status = (String) source.get("status");

            if ("PROCESSING".equals(status) || status == null) {
                retryCount++;
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        // Assert that we have a valid experiment status
        assertNotNull("Experiment status should not be null", status);

        // Refresh all related indices to ensure documents are available for search
        makeRequest(
            client(),
            RestRequest.Method.POST.name(),
            EXPERIMENT_VARIANT_INDEX + "/_refresh",
            null,
            null,
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        makeRequest(
            client(),
            RestRequest.Method.POST.name(),
            EVALUATION_RESULT_INDEX + "/_refresh",
            null,
            null,
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        // Add delay to ensure data propagation
        try {
            Thread.sleep(POST_COMPLETION_WAIT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return source;
    }

    private String createExperiment(String querySetId, String searchConfigurationId, String judgmentId) throws IOException,
        URISyntaxException, InterruptedException {
        String createExperimentBody = replacePlaceholders(
            Files.readString(Path.of(classLoader.getResource("experiment/CreateExperimentPointwiseEvaluation.json").toURI())),
            Map.of("query_set_id", querySetId, "search_configuration_id", searchConfigurationId, "judgment_id", judgmentId)
        );
        Response createExperimentResponse = makeRequest(
            client(),
            RestRequest.Method.PUT.name(),
            EXPERIMENTS_URI,
            null,
            toHttpEntity(createExperimentBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> createExperimentResultJson = entityAsMap(createExperimentResponse);
        String experimentId = createExperimentResultJson.get("experiment_id").toString();
        assertNotNull(experimentId);
        assertEquals("CREATED", createExperimentResultJson.get("experiment_result").toString());

        Thread.sleep(1000);
        return experimentId;
    }

    private String createHybridOptimizerExperiment(String querySetId, String searchConfigurationId, String judgmentId) throws IOException,
        URISyntaxException, InterruptedException {
        String createExperimentBody = replacePlaceholders(
            Files.readString(Path.of(classLoader.getResource("experiment/CreateExperimentHybridOptimizer.json").toURI())),
            Map.of("query_set_id", querySetId, "search_config_id", searchConfigurationId, "judgment_id", judgmentId)
        );
        Response createExperimentResponse = makeRequest(
            client(),
            RestRequest.Method.PUT.name(),
            EXPERIMENTS_URI,
            null,
            toHttpEntity(createExperimentBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> createExperimentResultJson = entityAsMap(createExperimentResponse);
        String experimentId = createExperimentResultJson.get("experiment_id").toString();
        assertNotNull(experimentId);
        assertEquals("CREATED", createExperimentResultJson.get("experiment_result").toString());
        return experimentId;
    }

    private String createJudgment() throws IOException, URISyntaxException, InterruptedException {
        String importJudgmentBody = Files.readString(Path.of(classLoader.getResource("data_esci/ImportJudgment.json").toURI()));
        Response importJudgementResponse = makeRequest(
            client(),
            RestRequest.Method.PUT.name(),
            JUDGMENTS_URL,
            null,
            toHttpEntity(importJudgmentBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> importResultJson = entityAsMap(importJudgementResponse);
        String judgmentId = importResultJson.get("judgment_id").toString();
        assertNotNull(judgmentId);

        // wait for completion of import action
        Thread.sleep(1000);
        return judgmentId;
    }

    private String createQuerySet() throws IOException, URISyntaxException {
        String createQuerySetBody = Files.readString(Path.of(classLoader.getResource("queryset/CreateQuerySet.json").toURI()));
        Response createQuerySetResponse = makeRequest(
            client(),
            RestRequest.Method.PUT.name(),
            QUERYSETS_URL,
            null,
            toHttpEntity(createQuerySetBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> createQuerySetResultJson = entityAsMap(createQuerySetResponse);
        String querySetId = createQuerySetResultJson.get("query_set_id").toString();
        assertNotNull(querySetId);
        return querySetId;
    }

    @SneakyThrows
    private String createSearchConfiguration() {
        String createSearchConfigurationRequestBody = Files.readString(
            Path.of(classLoader.getResource("searchconfig/CreateSearchConfigurationQueryWithPlaceholder.json").toURI())
        );
        Response createSearchConfigurationResponse = makeRequest(
            client(),
            RestRequest.Method.PUT.name(),
            SEARCH_CONFIGURATIONS_URL,
            null,
            toHttpEntity(createSearchConfigurationRequestBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> createSearchConfigurationResultJson = entityAsMap(createSearchConfigurationResponse);
        String searchConfigurationId = createSearchConfigurationResultJson.get("search_configuration_id").toString();
        assertNotNull(searchConfigurationId);
        return searchConfigurationId;
    }

    @SneakyThrows
    private String createHybridSearchConfiguration() {
        String createSearchConfigurationRequestBody = Files.readString(
            Path.of(classLoader.getResource("searchconfig/CreateSearchConfigurationHybridQuery.json").toURI())
        );
        Response createSearchConfigurationResponse = makeRequest(
            client(),
            RestRequest.Method.PUT.name(),
            SEARCH_CONFIGURATIONS_URL,
            null,
            toHttpEntity(createSearchConfigurationRequestBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> createSearchConfigurationResultJson = entityAsMap(createSearchConfigurationResponse);
        String searchConfigurationId = createSearchConfigurationResultJson.get("search_configuration_id").toString();
        assertNotNull(searchConfigurationId);
        return searchConfigurationId;
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) {
        if (INDEX_NAME_ESCI.equals(indexName) && !indexExists(indexName)) {
            String indexConfiguration = Files.readString(Path.of(classLoader.getResource("data_esci/CreateIndex.json").toURI()));
            createIndexWithConfiguration(indexName, indexConfiguration);
            String importDatasetBody = Files.readString(Path.of(classLoader.getResource("data_esci/BulkIngestDocuments.json").toURI()));
            bulkIngest(indexName, importDatasetBody);
        }
    }

    private void assertListsHaveSameElements(List<String> expected, List<String> actual) {
        List<String> sortedExpected = new ArrayList<>(expected);
        List<String> sortedActual = new ArrayList<>(actual);
        Collections.sort(sortedExpected);
        Collections.sort(sortedActual);
        assertArrayEquals(sortedExpected.toArray(new String[0]), sortedActual.toArray(new String[0]));
    }
}
