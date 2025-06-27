/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.executors;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.searchrelevance.exception.SearchRelevanceException;
import org.opensearch.searchrelevance.indices.SearchRelevanceIndices;
import org.opensearch.searchrelevance.shared.StashedThreadContext;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

/**
 * Manages bulk write operations with batching, rate limiting, and retry logic
 * to prevent overwhelming the OpenSearch cluster
 */
@Log4j2
public class BulkWriteManager {

    // Configuration constants - optimized for performance
    private static final int DEFAULT_BATCH_SIZE = 50;
    private static final int DEFAULT_MAX_CONCURRENT_BATCHES = 4;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final Duration DEFAULT_RETRY_DELAY = Duration.ofMillis(1600); // Reduced from 2

    private final Client client;
    private final ThreadPool threadPool;
    private final int batchSize;
    private final int maxRetries;
    private final Duration retryDelay;
    private final Semaphore concurrencyControl;

    public BulkWriteManager(Client client, ThreadPool threadPool) {
        this(client, threadPool, DEFAULT_BATCH_SIZE, DEFAULT_MAX_CONCURRENT_BATCHES, DEFAULT_MAX_RETRIES);
    }

    public BulkWriteManager(Client client, ThreadPool threadPool, int batchSize, int maxConcurrentBatches, int maxRetries) {
        this.client = client;
        this.threadPool = threadPool;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.retryDelay = DEFAULT_RETRY_DELAY;
        this.concurrencyControl = new Semaphore(maxConcurrentBatches, true);

        log.info(
            "BulkWriteManager initialized with batch size: {}, max concurrent batches: {}, max retries: {}",
            batchSize,
            maxConcurrentBatches,
            maxRetries
        );
    }

    /**
     * Execute bulk write operations with batching and retry logic
     */
    public CompletableFuture<BulkWriteResult> executeBulkWrite(List<BulkWriteItem> items) {
        if (items == null || items.isEmpty()) {
            return CompletableFuture.completedFuture(BulkWriteResult.builder().successCount(0).failureCount(0).totalItems(0).build());
        }

        CompletableFuture<BulkWriteResult> resultFuture = new CompletableFuture<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger completedBatches = new AtomicInteger(0);

        // Split items into batches
        List<List<BulkWriteItem>> batches = createBatches(items);
        int totalBatches = batches.size();

        log.debug("Executing bulk write for {} items in {} batches", items.size(), totalBatches);

        // Process each batch
        for (List<BulkWriteItem> batch : batches) {
            processBatch(batch, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    // Count successes and failures in this batch
                    int batchSuccesses = 0;
                    int batchFailures = 0;

                    if (bulkResponse.hasFailures()) {
                        for (int i = 0; i < bulkResponse.getItems().length; i++) {
                            if (bulkResponse.getItems()[i].isFailed()) {
                                batchFailures++;
                                log.warn("Bulk write item failed: {}", bulkResponse.getItems()[i].getFailureMessage());
                            } else {
                                batchSuccesses++;
                            }
                        }
                    } else {
                        batchSuccesses = batch.size();
                    }

                    successCount.addAndGet(batchSuccesses);
                    failureCount.addAndGet(batchFailures);

                    // Check if all batches are complete
                    if (completedBatches.incrementAndGet() == totalBatches) {
                        resultFuture.complete(
                            BulkWriteResult.builder()
                                .successCount(successCount.get())
                                .failureCount(failureCount.get())
                                .totalItems(items.size())
                                .build()
                        );
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    failureCount.addAndGet(batch.size());
                    log.error("Bulk write batch failed completely: {}", e.getMessage());

                    // Check if all batches are complete
                    if (completedBatches.incrementAndGet() == totalBatches) {
                        resultFuture.complete(
                            BulkWriteResult.builder()
                                .successCount(successCount.get())
                                .failureCount(failureCount.get())
                                .totalItems(items.size())
                                .build()
                        );
                    }
                }
            });
        }

        return resultFuture;
    }

    /**
     * Process a single batch with retry logic and concurrency control
     */
    private void processBatch(List<BulkWriteItem> batch, ActionListener<BulkResponse> listener) {
        processBatchWithRetry(batch, listener, 0);
    }

    private void processBatchWithRetry(List<BulkWriteItem> batch, ActionListener<BulkResponse> listener, int attempt) {
        // Try to acquire semaphore permit
        if (concurrencyControl.tryAcquire()) {
            try {
                threadPool.executor(HybridSearchTaskManager.THREAD_POOL_EXECUTOR_NAME).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        concurrencyControl.release();
                        handleBatchFailure(batch, listener, attempt, e);
                    }

                    @Override
                    protected void doRun() {
                        try {
                            executeBatch(batch, new ActionListener<BulkResponse>() {
                                @Override
                                public void onResponse(BulkResponse bulkResponse) {
                                    concurrencyControl.release();
                                    listener.onResponse(bulkResponse);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    concurrencyControl.release();
                                    handleBatchFailure(batch, listener, attempt, e);
                                }
                            });
                        } catch (Exception e) {
                            concurrencyControl.release();
                            throw e;
                        }
                    }
                });
            } catch (Exception e) {
                concurrencyControl.release();
                handleBatchFailure(batch, listener, attempt, e);
            }
        } else {
            // No permit available, schedule retry
            threadPool.schedule(
                () -> { processBatchWithRetry(batch, listener, attempt); },
                new TimeValue(retryDelay.toMillis()),
                HybridSearchTaskManager.THREAD_POOL_EXECUTOR_NAME
            );
        }
    }

    private void handleBatchFailure(List<BulkWriteItem> batch, ActionListener<BulkResponse> listener, int attempt, Exception e) {
        if (attempt < maxRetries && isRetryableException(e)) {
            log.warn("Batch write failed (attempt {}), retrying: {}", attempt + 1, e.getMessage());

            // Exponential backoff
            long delayMs = retryDelay.toMillis() * (1L << attempt);
            threadPool.schedule(
                () -> { processBatchWithRetry(batch, listener, attempt + 1); },
                new TimeValue(delayMs),
                HybridSearchTaskManager.THREAD_POOL_EXECUTOR_NAME
            );
        } else {
            log.error("Batch write failed after {} attempts: {}", attempt + 1, e.getMessage());
            listener.onFailure(e);
        }
    }

    private boolean isRetryableException(Exception e) {
        String message = e.getMessage();
        if (message != null) {
            return message.contains("too_many_requests")
                || message.contains("circuit_breaking_exception")
                || message.contains("timeout")
                || message.contains("unavailable");
        }
        return false;
    }

    private void executeBatch(List<BulkWriteItem> batch, ActionListener<BulkResponse> listener) {
        BulkRequest bulkRequest = new BulkRequest();

        // Use WAIT_UNTIL refresh policy instead of IMMEDIATE to reduce pressure
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        for (BulkWriteItem item : batch) {
            try {
                IndexRequest indexRequest = new IndexRequest(item.getIndex().getIndexName()).id(item.getDocumentId())
                    .source(item.getXContentBuilder());

                bulkRequest.add(indexRequest);
            } catch (Exception e) {
                log.error("Failed to add item to bulk request: {}", e.getMessage());
                listener.onFailure(new SearchRelevanceException("Failed to prepare bulk request", e, RestStatus.INTERNAL_SERVER_ERROR));
                return;
            }
        }

        StashedThreadContext.run(client, () -> { client.bulk(bulkRequest, listener); });
    }

    private List<List<BulkWriteItem>> createBatches(List<BulkWriteItem> items) {
        List<List<BulkWriteItem>> batches = new ArrayList<>();

        for (int i = 0; i < items.size(); i += batchSize) {
            int end = Math.min(i + batchSize, items.size());
            batches.add(new ArrayList<>(items.subList(i, end)));
        }

        return batches;
    }

    /**
     * Represents an item to be written in bulk operation
     */
    @Builder
    @Getter
    public static class BulkWriteItem {
        private final String documentId;
        private final XContentBuilder xContentBuilder;
        private final SearchRelevanceIndices index;
    }

    /**
     * Result of bulk write operation
     */
    @Builder
    @Getter
    public static class BulkWriteResult {
        private final int successCount;
        private final int failureCount;
        private final int totalItems;

        public boolean hasFailures() {
            return failureCount > 0;
        }

        public double getSuccessRate() {
            return totalItems > 0 ? (double) successCount / totalItems : 0.0;
        }
    }
}
