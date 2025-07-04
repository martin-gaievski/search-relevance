/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.ml;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.rest.RestRequest;
import org.opensearch.searchrelevance.BaseSearchRelevanceIT;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.google.common.collect.ImmutableList;

import lombok.SneakyThrows;

/**
 * Integration tests for LLM judgment functionality with ML Commons.
 * This test demonstrates setting up ML Commons with a remote LLM connector,
 * registering and deploying a model, and making prediction calls.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class LLMJudgmentIT extends BaseSearchRelevanceIT {

    private static final String ML_COMMONS_CONNECTORS_URL = "/_plugins/_ml/connectors/_create";
    private static final String ML_COMMONS_MODELS_REGISTER_URL = "/_plugins/_ml/models/_register";
    private static final String ML_COMMONS_MODELS_DEPLOY_URL = "/_plugins/_ml/models/%s/_deploy";
    private static final String ML_COMMONS_TASKS_URL = "/_plugins/_ml/tasks/%s";
    private static final String ML_COMMONS_MODELS_URL = "/_plugins/_ml/models/%s";
    private static final String ML_COMMONS_PREDICT_URL = "/_plugins/_ml/_predict/%s/%s";
    private static final String CLUSTER_SETTINGS_URL = "/_cluster/settings";

    private static final int MAX_WAIT_TIME_MS = 60000;
    private static final int POLL_INTERVAL_MS = 1000;

    private String connectorId;
    private String modelId;

    @Before
    @SneakyThrows
    public void setupMLCommons() {
        // Only configure ML Commons if LLM testing is enabled
        if (!isLLMTestingEnabled()) {
            return;
        }

        configureMLCommonsSettings();
        Thread.sleep(DEFAULT_INTERVAL_MS);
    }

    @SneakyThrows
    public void testMLCommonsLLMIntegration() {
        // Skip test if LLM testing is not enabled
        if (!isLLMTestingEnabled()) {
            logger.info("Skipping LLM test - LLM testing not enabled");
            return;
        }

        try {
            // Step 1: Create remote connector
            connectorId = createRemoteConnector();
            assertNotNull("Connector ID should not be null", connectorId);
            logger.info("Created connector with ID: " + connectorId);

            // Step 2: Register model
            String registerTaskId = registerModel(connectorId);
            assertNotNull("Register task ID should not be null", registerTaskId);

            // Step 3: Wait for registration to complete and get model ID
            modelId = waitForTaskCompletionAndGetModelId(registerTaskId);
            assertNotNull("Model ID should not be null", modelId);
            logger.info("Registered model with ID: " + modelId);

            // Step 4: Deploy model
            String deployTaskId = deployModel(modelId);
            assertNotNull("Deploy task ID should not be null", deployTaskId);

            // Step 5: Wait for deployment to complete
            waitForTaskCompletion(deployTaskId);
            waitForModelDeployment(modelId);
            logger.info("Model deployed successfully");

            // Step 6: Test prediction
            testModelPrediction(modelId);
            logger.info("Model prediction test successful");

        } catch (Exception e) {
            logger.error("Test failed with exception: ", e);
            throw e;
        }
    }

    private void configureMLCommonsSettings() throws IOException {
        String settingsBody = "{\n"
            + "  \"persistent\": {\n"
            + "    \"plugins.ml_commons.only_run_on_ml_node\": false,\n"
            + "    \"plugins.ml_commons.model_access_control_enabled\": false,\n"
            + "    \"plugins.ml_commons.connector_access_control_enabled\": false,\n"
            + "    \"plugins.ml_commons.connector.private_ip_enabled\": true,\n"
            + "    \"plugins.ml_commons.native_memory_threshold\": 99,\n"
            + "    \"plugins.ml_commons.allow_registering_model_via_url\": true,\n"
            + "    \"plugins.ml_commons.allow_registering_model_via_local_file\": true,\n"
            + "    \"plugins.ml_commons.trusted_connector_endpoints_regex\": [\n"
            + "      \"^https://runtime\\\\.sagemaker\\\\..*\\\\.amazonaws\\\\.com/.*\",\n"
            + "      \"^https://api\\\\.openai\\\\.com/.*\",\n"
            + "      \"^https://api\\\\.cohere\\\\.ai/.*\",\n"
            + "      \"^https://.*\\\\.openai\\\\.azure\\\\.com/.*\",\n"
            + "      \"^https://api\\\\.anthropic\\\\.com/.*\",\n"
            + "      \"^https://bedrock-runtime\\\\..*\\\\.amazonaws\\\\.com/.*\",\n"
            + "      \"^http://localhost:.*\",\n"
            + "      \"^https://localhost:.*\",\n"
            + "      \"^http://127\\\\.0\\\\.0\\\\.1:.*\",\n"
            + "      \"^https://127\\\\.0\\\\.0\\\\.1:.*\",\n"
            + "      \"^http://host\\\\.docker\\\\.internal:.*\",\n"
            + "      \"^https://host\\\\.docker\\\\.internal:.*\"\n"
            + "    ],\n"
            + "    \"plugins.ml_commons.trusted_url_regex\": [\n"
            + "      \"^http://localhost:.*\",\n"
            + "      \"^https://localhost:.*\",\n"
            + "      \"^http://127\\\\.0\\\\.0\\\\.1:.*\",\n"
            + "      \"^https://127\\\\.0\\\\.0\\\\.1:.*\",\n"
            + "      \"^http://host\\\\.docker\\\\.internal:.*\",\n"
            + "      \"^https://host\\\\.docker\\\\.internal:.*\"\n"
            + "    ]\n"
            + "  }\n"
            + "}";

        Response response = makeRequest(
            adminClient(),
            RestRequest.Method.PUT.name(),
            CLUSTER_SETTINGS_URL,
            null,
            toHttpEntity(settingsBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        assertEquals("Cluster settings update should be successful", 200, response.getStatusLine().getStatusCode());
    }

    private String createRemoteConnector() throws IOException {
        String llmApiUrl = getLLMApiUrl();
        String modelName = getLLMModelName();

        String connectorBody = "{\n"
            + "  \"name\": \"LLM POC Connector\",\n"
            + "  \"description\": \"Connector for LLM testing\",\n"
            + "  \"version\": \"1.0.0\",\n"
            + "  \"protocol\": \"http\",\n"
            + "  \"parameters\": {\n"
            + "    \"endpoint\": \"" + llmApiUrl + "\",\n"
            + "    \"model\": \"" + modelName + "\"\n"
            + "  },\n"
            + "  \"credential\": {\n"
            + "    \"access_key\": \"\",\n"
            + "    \"secret_key\": \"\"\n"
            + "  },\n"
            + "  \"actions\": [\n"
            + "    {\n"
            + "      \"action_type\": \"predict\",\n"
            + "      \"method\": \"POST\",\n"
            + "      \"url\": \"" + llmApiUrl + "/v1/chat/completions\",\n"
            + "      \"headers\": {\n"
            + "        \"Content-Type\": \"application/json\"\n"
            + "      },\n"
            + "      \"request_body\": \"{ \\\"model\\\": \\\"" + modelName + "\\\", "
            + "\\\"messages\\\": ${parameters.messages}, "
            + "\\\"temperature\\\": 0.1, "
            + "\\\"max_tokens\\\": 1000, "
            + "\\\"stream\\\": false }\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";

        Response response = makeRequest(
            client(),
            RestRequest.Method.POST.name(),
            ML_COMMONS_CONNECTORS_URL,
            null,
            toHttpEntity(connectorBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        Map<String, Object> responseMap = entityAsMap(response);
        return (String) responseMap.get("connector_id");
    }

    private String registerModel(String connectorId) throws IOException {
        String registerBody = "{\n"
            + "  \"name\": \"LLM POC Model\",\n"
            + "  \"function_name\": \"remote\",\n"
            + "  \"connector_id\": \"" + connectorId + "\"\n"
            + "}";

        Response response = makeRequest(
            client(),
            RestRequest.Method.POST.name(),
            ML_COMMONS_MODELS_REGISTER_URL,
            null,
            toHttpEntity(registerBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        Map<String, Object> responseMap = entityAsMap(response);
        return (String) responseMap.get("task_id");
    }

    private String deployModel(String modelId) throws IOException {
        Response response = makeRequest(
            client(),
            RestRequest.Method.POST.name(),
            String.format(ML_COMMONS_MODELS_DEPLOY_URL, modelId),
            null,
            null,
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        Map<String, Object> responseMap = entityAsMap(response);
        return (String) responseMap.get("task_id");
    }

    private void testModelPrediction(String modelId) throws IOException {
        String predictBody = "{\n"
            + "  \"parameters\": {\n"
            + "    \"messages\": [\n"
            + "      {\n"
            + "        \"role\": \"system\",\n"
            + "        \"content\": \"You are a helpful assistant that evaluates search relevance.\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"role\": \"user\",\n"
            + "        \"content\": \"Rate the relevance of 'iPhone 15 Pro' to the query 'smartphone with good camera' on a scale of 0 to 1.\"\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";

        Response response = makeRequest(
            client(),
            RestRequest.Method.POST.name(),
            String.format(ML_COMMONS_PREDICT_URL, "remote", modelId),
            null,
            toHttpEntity(predictBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        Map<String, Object> responseMap = entityAsMap(response);
        assertNotNull("Prediction response should not be null", responseMap);

        // Verify the response structure
        assertTrue("Response should contain inference_results", responseMap.containsKey("inference_results"));
        List<Map<String, Object>> inferenceResults = (List<Map<String, Object>>) responseMap.get("inference_results");
        assertNotNull("Inference results should not be null", inferenceResults);
        assertFalse("Inference results should not be empty", inferenceResults.isEmpty());

        Map<String, Object> firstResult = inferenceResults.get(0);
        assertNotNull("First inference result should not be null", firstResult);
        assertTrue("Result should contain output", firstResult.containsKey("output"));

        List<Map<String, Object>> outputs = (List<Map<String, Object>>) firstResult.get("output");
        assertNotNull("Outputs should not be null", outputs);
        assertFalse("Outputs should not be empty", outputs.isEmpty());

        logger.info("Prediction response: " + responseMap);
    }

    private String waitForTaskCompletionAndGetModelId(String taskId) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        String modelId = null;

        while (System.currentTimeMillis() - startTime < MAX_WAIT_TIME_MS) {
            Response response = makeRequest(
                client(),
                RestRequest.Method.GET.name(),
                String.format(ML_COMMONS_TASKS_URL, taskId),
                null,
                null,
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
            );

            Map<String, Object> responseMap = entityAsMap(response);
            String state = (String) responseMap.get("state");

            if ("COMPLETED".equals(state)) {
                modelId = (String) responseMap.get("model_id");
                break;
            } else if ("FAILED".equals(state)) {
                String error = (String) responseMap.get("error");
                fail("Task failed with error: " + error);
            }

            Thread.sleep(POLL_INTERVAL_MS);
        }

        if (modelId == null) {
            fail("Task did not complete within timeout period");
        }

        return modelId;
    }

    private void waitForTaskCompletion(String taskId) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < MAX_WAIT_TIME_MS) {
            Response response = makeRequest(
                client(),
                RestRequest.Method.GET.name(),
                String.format(ML_COMMONS_TASKS_URL, taskId),
                null,
                null,
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
            );

            Map<String, Object> responseMap = entityAsMap(response);
            String state = (String) responseMap.get("state");

            if ("COMPLETED".equals(state)) {
                return;
            } else if ("FAILED".equals(state)) {
                String error = (String) responseMap.get("error");
                fail("Task failed with error: " + error);
            }

            Thread.sleep(POLL_INTERVAL_MS);
        }

        fail("Task did not complete within timeout period");
    }

    private void waitForModelDeployment(String modelId) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < MAX_WAIT_TIME_MS) {
            Response response = makeRequest(
                client(),
                RestRequest.Method.GET.name(),
                String.format(ML_COMMONS_MODELS_URL, modelId),
                null,
                null,
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
            );

            Map<String, Object> responseMap = entityAsMap(response);
            String state = (String) responseMap.get("model_state");

            if ("DEPLOYED".equals(state)) {
                return;
            } else if ("DEPLOY_FAILED".equals(state)) {
                fail("Model deployment failed");
            }

            Thread.sleep(POLL_INTERVAL_MS);
        }

        fail("Model did not deploy within timeout period");
    }

    private boolean isLLMTestingEnabled() {
        // Check if LLM testing is enabled via system property
        String llmEnabled = System.getProperty("tests.cluster.llm.enabled", "false");
        return "true".equalsIgnoreCase(llmEnabled);
    }

    private String getLLMApiUrl() {
        // Get LLM API URL from environment or use default
        String apiUrl = System.getenv("LOCALAI_API_URL");
        if (apiUrl == null || apiUrl.isEmpty()) {
            apiUrl = System.getProperty("tests.cluster.llm.api.url", "http://localhost:8080");
        }
        return apiUrl;
    }

    private String getLLMModelName() {
        // Get LLM model name from environment or use default
        String modelName = System.getenv("LLM_MODEL_NAME");
        if (modelName == null || modelName.isEmpty()) {
            modelName = System.getProperty("tests.cluster.llm.model.name", "phi-2");
        }
        return modelName;
    }
}
