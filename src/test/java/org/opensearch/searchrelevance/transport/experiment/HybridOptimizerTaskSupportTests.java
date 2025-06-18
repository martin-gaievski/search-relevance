/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.transport.experiment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.searchrelevance.dao.ExperimentVariantDao;
import org.opensearch.searchrelevance.dao.JudgmentDao;
import org.opensearch.searchrelevance.metrics.HybridSearchTaskManager;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for HybridOptimizerTaskSupport
 */
public class HybridOptimizerTaskSupportTests extends OpenSearchTestCase {

    private JudgmentDao judgmentDao;
    private ExperimentVariantDao experimentVariantDao;
    private HybridSearchTaskManager taskManager;
    private HybridOptimizerTaskSupport taskSupport;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        judgmentDao = mock(JudgmentDao.class);
        experimentVariantDao = mock(ExperimentVariantDao.class);
        taskManager = mock(HybridSearchTaskManager.class);
        taskSupport = new HybridOptimizerTaskSupport(judgmentDao, experimentVariantDao, taskManager);
    }

    public void testProcessHybridOptimizerExperimentWithJudgmentFailureShouldHandleGracefully() {
        // Arrange
        String experimentId = "exp-123";
        String queryText = "test query";
        Map<String, List<String>> indexAndQueries = Map.of("config1", Arrays.asList("index1", "{\"match_all\":{}}"));
        List<String> judgmentList = Arrays.asList("judgment1");
        int size = 10;
        List<Map<String, Object>> finalResults = new java.util.ArrayList<>();
        AtomicInteger pendingQueries = new AtomicInteger(1);
        AtomicBoolean hasFailure = new AtomicBoolean(false);
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

        // Mock judgment failure
        doAnswer(invocation -> {
            ActionListener<SearchResponse> judgmentListener = invocation.getArgument(1);
            judgmentListener.onFailure(new RuntimeException("Judgment fetch failed"));
            return null;
        }).when(judgmentDao).getJudgment(anyString(), any(ActionListener.class));

        // Act
        taskSupport.processHybridOptimizerExperiment(
            experimentId,
            queryText,
            indexAndQueries,
            judgmentList,
            size,
            finalResults,
            pendingQueries,
            hasFailure,
            listener
        );

        // Assert
        verify(judgmentDao, times(1)).getJudgment(anyString(), any(ActionListener.class));
        assertTrue("Failure listener should be called", failureCalled.get());
        verify(taskManager, never()).scheduleTasks(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(Integer.class),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any()
        );
    }
}
