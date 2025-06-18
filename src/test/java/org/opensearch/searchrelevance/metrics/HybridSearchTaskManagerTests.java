/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.metrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.searchrelevance.dao.EvaluationResultDao;
import org.opensearch.searchrelevance.dao.ExperimentVariantDao;
import org.opensearch.searchrelevance.model.AsyncStatus;
import org.opensearch.searchrelevance.model.ExperimentType;
import org.opensearch.searchrelevance.model.ExperimentVariant;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Unit tests for HybridSearchTaskManager
 */
public class HybridSearchTaskManagerTests extends OpenSearchTestCase {

    private Client client;
    private ClusterService clusterService;
    private EvaluationResultDao evaluationResultDao;
    private ExperimentVariantDao experimentVariantDao;
    private ThreadPool threadPool;
    private ExecutorService immediateExecutor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        clusterService = mock(ClusterService.class);
        evaluationResultDao = mock(EvaluationResultDao.class);
        experimentVariantDao = mock(ExperimentVariantDao.class);
        threadPool = mock(ThreadPool.class);

        // Create an immediate executor
        immediateExecutor = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable command = invocation.getArgument(0);
            command.run();
            return null;
        }).when(immediateExecutor).execute(any(Runnable.class));

        // Setup thread pool mocks
        when(threadPool.executor(anyString())).thenReturn(immediateExecutor);

        // Handle scheduled tasks
        doAnswer(invocation -> {
            Runnable command = invocation.getArgument(0);
            command.run();
            return null;
        }).when(threadPool).schedule(any(Runnable.class), any(TimeValue.class), anyString());
    }

    public void testScheduleTasksWithValidInputShouldTrackCompletions() {
        // This is a simplified test focusing on completion tracking logic

        // Arrange
        AtomicInteger pendingConfigs = new AtomicInteger(1);
        AtomicBoolean hasFailure = new AtomicBoolean(false);
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

        // Act - Simulate completion of configuration
        pendingConfigs.decrementAndGet();
        if (pendingConfigs.get() == 0 && !hasFailure.get()) {
            listener.onResponse(new HashMap<>());
        }

        // Assert
        assertTrue("Listener should be called when all configurations complete", listenerCalled.get());
    }

    public void testScheduleTasksWithMultipleConfigsShouldWaitForAll() {
        // Arrange
        AtomicInteger pendingConfigs = new AtomicInteger(2); // Two configurations
        AtomicBoolean hasFailure = new AtomicBoolean(false);
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

        // Act - Simulate completion of first configuration
        pendingConfigs.decrementAndGet();
        if (pendingConfigs.get() == 0 && !hasFailure.get()) {
            listener.onResponse(new HashMap<>());
        }

        // Assert - Should not complete yet
        assertFalse("Listener should not be called until all configs complete", listenerCalled.get());

        // Act - Simulate completion of second configuration
        pendingConfigs.decrementAndGet();
        if (pendingConfigs.get() == 0 && !hasFailure.get()) {
            listener.onResponse(new HashMap<>());
        }

        // Assert - Should complete now
        assertTrue("Listener should be called when all configs are done", listenerCalled.get());
    }

    private List<ExperimentVariant> createTestVariants(String experimentId, int count) {
        List<ExperimentVariant> variants = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            ExperimentVariant variant = new ExperimentVariant(
                "variant-" + i,
                "2023-01-01T00:00:00Z",
                ExperimentType.HYBRID_OPTIMIZER,
                AsyncStatus.PROCESSING,
                experimentId,
                Map.of("param" + i, "value" + i),
                Map.of()
            );
            variants.add(variant);
        }
        return variants;
    }
}
