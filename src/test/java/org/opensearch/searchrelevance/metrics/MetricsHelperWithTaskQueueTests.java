/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.metrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.searchrelevance.dao.EvaluationResultDao;
import org.opensearch.searchrelevance.dao.ExperimentVariantDao;
import org.opensearch.searchrelevance.dao.JudgmentDao;
import org.opensearch.searchrelevance.model.AsyncStatus;
import org.opensearch.searchrelevance.model.ExperimentType;
import org.opensearch.searchrelevance.model.ExperimentVariant;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

/**
 * Unit tests for MetricsHelperWithTaskQueue
 */
public class MetricsHelperWithTaskQueueTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private Client client;
    private JudgmentDao judgmentDao;
    private EvaluationResultDao evaluationResultDao;
    private ExperimentVariantDao experimentVariantDao;
    private HybridSearchTaskManager hybridSearchTaskManager;
    private MetricsHelperWithTaskQueue metricsHelper;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        judgmentDao = mock(JudgmentDao.class);
        evaluationResultDao = mock(EvaluationResultDao.class);
        experimentVariantDao = mock(ExperimentVariantDao.class);
        hybridSearchTaskManager = mock(HybridSearchTaskManager.class);

        metricsHelper = new MetricsHelperWithTaskQueue(
            clusterService,
            client,
            judgmentDao,
            evaluationResultDao,
            experimentVariantDao,
            hybridSearchTaskManager
        );
    }

    public void testProcessEvaluationMetricsWithEmptyVariantsShouldCallParent() {
        // Arrange
        String queryText = "test query";
        Map<String, List<String>> indexAndQueries = Map.of("config1", Arrays.asList("index1", "{\"match_all\":{}}"));
        int size = 10;
        List<String> judgmentIds = Arrays.asList("judgment1");
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        ActionListener<Map<String, Object>> listener = new ActionListener<>() {
            @Override
            public void onResponse(Map<String, Object> response) {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        };
        List<ExperimentVariant> emptyVariants = Arrays.asList();

        // Mock responses for parent method calls
        doAnswer(invocation -> {
            ActionListener<SearchResponse> searchListener = invocation.getArgument(1);
            searchListener.onResponse(mock(SearchResponse.class));
            return null;
        }).when(client).search(any(), any(ActionListener.class));

        // Act
        metricsHelper.processEvaluationMetrics(queryText, indexAndQueries, size, judgmentIds, listener, emptyVariants);

        // No need for assertions - we're just verifying the method correctly delegates
        // and doesn't throw exceptions
    }

    public void testProcessEvaluationMetricsWithEmptyIndexAndQueriesShouldFailWithException() {
        // Arrange
        String queryText = "test query";
        Map<String, List<String>> emptyIndexAndQueries = new HashMap<>();
        int size = 10;
        List<String> judgmentIds = Arrays.asList("judgment1");
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        ActionListener<Map<String, Object>> listener = new ActionListener<>() {
            @Override
            public void onResponse(Map<String, Object> response) {
                fail("Should not succeed");
            }

            @Override
            public void onFailure(Exception e) {
                failureCalled.set(true);
            }
        };
        List<ExperimentVariant> variants = createTestVariants();

        // Act
        metricsHelper.processEvaluationMetrics(queryText, emptyIndexAndQueries, size, judgmentIds, listener, variants);

        // Assert
        assertTrue("Listener failure should be called for empty index and queries", failureCalled.get());
    }

    public void testProcessEvaluationMetricsWithEmptyJudgmentIdsShouldFailWithException() {
        // Arrange
        String queryText = "test query";
        Map<String, List<String>> indexAndQueries = Map.of("config1", Arrays.asList("index1", "{\"match_all\":{}}"));
        int size = 10;
        List<String> emptyJudgmentIds = Arrays.asList();
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        ActionListener<Map<String, Object>> listener = new ActionListener<>() {
            @Override
            public void onResponse(Map<String, Object> response) {
                fail("Should not succeed");
            }

            @Override
            public void onFailure(Exception e) {
                failureCalled.set(true);
            }
        };
        List<ExperimentVariant> variants = createTestVariants();

        // Act
        metricsHelper.processEvaluationMetrics(queryText, indexAndQueries, size, emptyJudgmentIds, listener, variants);

        // Assert
        assertTrue("Listener failure should be called for empty judgment IDs", failureCalled.get());
    }

    public void testProcessHybridExperimentOptionsWithValidInputShouldDelegateToTaskManager() {
        // Arrange
        String queryText = "test query";
        int size = 10;
        List<String> judgmentIds = Arrays.asList("judgment1");
        Map<String, String> docIdToScores = Map.of("doc1", "3");
        Map<String, Object> configToExperimentVariants = new HashMap<>();
        AtomicBoolean responseCalled = new AtomicBoolean(false);

        ActionListener<Map<String, Object>> listener = new ActionListener<>() {
            @Override
            public void onResponse(Map<String, Object> response) {
                responseCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        };

        String searchConfigurationId = "config1";
        String index = "index1";
        String query = "{\"match_all\":{}}";
        AtomicBoolean hasFailure = new AtomicBoolean(false);
        AtomicInteger pendingConfigurations = new AtomicInteger(1);
        List<ExperimentVariant> experimentVariants = createTestVariants();

        // Mock task manager response
        doAnswer(invocation -> {
            ActionListener<Map<String, Object>> taskListener = invocation.getArgument(10);
            Map<String, Object> mockResults = new HashMap<>();
            mockResults.put("evaluationResults", Arrays.asList());
            taskListener.onResponse(mockResults);
            return null;
        }).when(hybridSearchTaskManager)
            .scheduleTasks(
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyInt(),
                any(),
                any(),
                any(),
                any(),
                any(ActionListener.class),
                any(),
                any()
            );

        // Act
        metricsHelper.processHybridExperimentOptions(
            queryText,
            size,
            judgmentIds,
            docIdToScores,
            configToExperimentVariants,
            listener,
            searchConfigurationId,
            index,
            query,
            hasFailure,
            pendingConfigurations,
            experimentVariants
        );

        // Assert
        verify(hybridSearchTaskManager, times(1)).scheduleTasks(
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            anyInt(),
            any(),
            any(),
            any(),
            any(),
            any(ActionListener.class),
            any(),
            any()
        );
        assertTrue("Response listener should be called", responseCalled.get());
    }

    public void testProcessHybridExperimentOptionsWithEmptyVariantsShouldThrowException() {
        // Arrange
        String queryText = "test query";
        int size = 10;
        List<String> judgmentIds = Arrays.asList("judgment1");
        Map<String, String> docIdToScores = Map.of("doc1", "3");
        Map<String, Object> configToExperimentVariants = new HashMap<>();

        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);

        String searchConfigurationId = "config1";
        String index = "index1";
        String query = "{\"match_all\":{}}";
        AtomicBoolean hasFailure = new AtomicBoolean(false);
        AtomicInteger pendingConfigurations = new AtomicInteger(1);
        List<ExperimentVariant> emptyVariants = Arrays.asList();

        // Act & Assert
        Exception exception = expectThrows(IllegalArgumentException.class, () -> {
            metricsHelper.processHybridExperimentOptions(
                queryText,
                size,
                judgmentIds,
                docIdToScores,
                configToExperimentVariants,
                listener,
                searchConfigurationId,
                index,
                query,
                hasFailure,
                pendingConfigurations,
                emptyVariants
            );
        });

        assertTrue(
            "Exception message should mention empty variants",
            exception.getMessage().contains("Experiment variant for hybrid search cannot be empty")
        );

        verify(hybridSearchTaskManager, times(0)).scheduleTasks(
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            anyInt(),
            any(),
            any(),
            any(),
            any(),
            any(ActionListener.class),
            any(),
            any()
        );
    }

    private List<ExperimentVariant> createTestVariants() {
        ExperimentVariant variant1 = new ExperimentVariant(
            "variant-1",
            "2023-01-01T00:00:00Z",
            ExperimentType.HYBRID_OPTIMIZER,
            AsyncStatus.PROCESSING,
            "exp-123",
            Map.of("param1", "value1"),
            Map.of()
        );

        ExperimentVariant variant2 = new ExperimentVariant(
            "variant-2",
            "2023-01-01T00:00:00Z",
            ExperimentType.HYBRID_OPTIMIZER,
            AsyncStatus.PROCESSING,
            "exp-123",
            Map.of("param2", "value2"),
            Map.of()
        );

        return Arrays.asList(variant1, variant2);
    }
}
