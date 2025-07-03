/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.experiment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.searchrelevance.dao.JudgmentDao;
import org.opensearch.searchrelevance.executors.ExperimentTaskManager;
import org.opensearch.searchrelevance.model.ExperimentType;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

/**
 * Tests for PointwiseExperimentProcessor
 */
public class PointwiseExperimentProcessorTests extends OpenSearchTestCase {

    @Mock
    private JudgmentDao judgmentDao;

    @Mock
    private ExperimentTaskManager taskManager;

    private PointwiseExperimentProcessor processor;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        processor = new PointwiseExperimentProcessor(judgmentDao, taskManager);
    }

    public void testProcessPointwiseExperiment_Success() {
        // Setup test data
        String experimentId = "test-experiment-id";
        String queryText = "test query";
        Map<String, List<String>> indexAndQueries = new HashMap<>();
        indexAndQueries.put("config1", Arrays.asList("test-index", "test-query", "test-pipeline"));
        List<String> judgmentList = Arrays.asList("judgment1");
        int size = 10;
        AtomicBoolean hasFailure = new AtomicBoolean(false);

        // Mock successful judgment response with empty response (we just want to test the flow)
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchResponse mockResponse = mock(SearchResponse.class);
            listener.onResponse(mockResponse);
            return null;
        }).when(judgmentDao).getJudgment(anyString(), any(ActionListener.class));

        // Mock task manager response
        CompletableFuture<Map<String, Object>> mockFuture = CompletableFuture.completedFuture(
            Map.of("evaluationResults", List.of(Map.of("evaluationId", "eval1", "variantId", "var1")))
        );
        when(
            taskManager.scheduleTasksAsync(
                any(ExperimentType.class),
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                any(Integer.class),
                any(List.class),
                any(List.class),
                any(Map.class),
                any(Map.class),
                any(AtomicBoolean.class)
            )
        ).thenReturn(mockFuture);

        // Mock ActionListener
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);

        // Execute
        processor.processPointwiseExperiment(experimentId, queryText, indexAndQueries, judgmentList, size, hasFailure, listener);

        // Verify interactions
        verify(judgmentDao).getJudgment(anyString(), any(ActionListener.class));
        verify(listener).onResponse(any(Map.class));
    }

    public void testProcessPointwiseExperiment_JudgmentFailure() {
        // Setup test data
        String experimentId = "test-experiment-id";
        String queryText = "test query";
        Map<String, List<String>> indexAndQueries = new HashMap<>();
        indexAndQueries.put("config1", Arrays.asList("test-index", "test-query"));
        List<String> judgmentList = Arrays.asList("judgment1");
        int size = 10;
        AtomicBoolean hasFailure = new AtomicBoolean(false);

        // Mock judgment failure
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("Judgment fetch failed"));
            return null;
        }).when(judgmentDao).getJudgment(anyString(), any(ActionListener.class));

        // Mock ActionListener
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);

        // Execute
        processor.processPointwiseExperiment(experimentId, queryText, indexAndQueries, judgmentList, size, hasFailure, listener);

        // Verify failure handling
        verify(listener).onFailure(any(Exception.class));
    }

    public void testCreatePointwiseVariants() {
        // Test constructor to ensure processor is properly initialized
        assertNotNull("Processor should be initialized", processor);

        // Test that the processor has proper dependencies
        assertNotNull("JudgmentDao should be injected", judgmentDao);
        assertNotNull("TaskManager should be injected", taskManager);
    }
}
