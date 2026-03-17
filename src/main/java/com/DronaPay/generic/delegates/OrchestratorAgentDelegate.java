package com.DronaPay.generic.delegates;

import com.DronaPay.generic.services.ObjectStorageService;
import com.DronaPay.generic.storage.StorageProvider;
import com.DronaPay.generic.utils.TenantPropertiesUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.cibseven.bpm.engine.delegate.BpmnError;
import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.Expression;
import org.cibseven.bpm.engine.delegate.JavaDelegate;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * OrchestratorAgentDelegate — Generic Agent execution delegate for the Orchestrator loop.
 *
 * Configuration is injected via camunda:field bindings in the Element Template.
 * Fields containing ${variableName} placeholders are resolved at runtime against
 * the current process execution, allowing the Orchestrator to drive all values
 * dynamically by setting the corresponding process variables before routing here.
 *
 * camunda:field bindings (set in generic-orchestrator-agent-template.json):
 *   - agentId       : "${currentAgentId}"      — agent ID for this stage
 *   - baseUrl       : "dia"                    — base URL key (hardcoded)
 *   - route         : "agent"                  — API route (hardcoded)
 *   - authType      : "basicAuth"              — auth method (hardcoded)
 *   - method        : "POST"                   — HTTP method (hardcoded)
 *   - inputParams   : "${currentInputParams}"  — JSON array describing the data payload
 *   - outputMapping : "${currentOutputMapping}"— JSON array for output routing
 *
 * Process variables consumed (set by OrchestratorDelegate before routing here):
 *   - currentAgentId      : String — e.g. "execution-mode-agent"
 *   - currentStageType    : String — e.g. "execution-mode" (used for MinIO folder name)
 *   - currentInputParams  : String — JSON array with MinIO paths already resolved by OrchestratorDelegate
 *   - currentOutputMapping: String — JSON array, e.g. [{"type":"var","key":"agentAction",...}]
 *
 * Process variables set (read by OrchestratorDelegate on next run):
 *   - lastAgentResult : String — full raw DIA JSON response (always set)
 *
 * Additional process variables set per outputMapping entries with type="var":
 *   - agentAction, agentStatus, agentQuestions, agentMissingFields, agentConfidence
 *
 * MinIO files written per outputMapping entries with type="minio":
 *   - {tenantId}/RuleCreation/{ticketId}/{Stage Folder}/{key}_run{n}.json
 *
 * inputParams array entry types:
 *   - type="minioJson" : fetches file at MinIO path in "value", extracts "sourceJsonPath", injects under "key"
 *   - type="value"     : uses literal "value" directly under "key"
 *
 * DIA request payload format:
 *   POST {agent.api.url}/agent
 *   { "agentid": "<agentId>", "data": { <built from inputParams> } }
 */
@Slf4j
public class OrchestratorAgentDelegate implements JavaDelegate {

    private static final String WORKFLOW_KEY = "RuleCreation";
    private static final String DIA_PROP_KEY = "agent.api";

    // ── camunda:field bindings injected from Element Template ─────────────────
    private Expression agentId;
    private Expression baseUrl;
    private Expression route;
    private Expression authType;
    private Expression method;
    private Expression inputParams;
    private Expression outputMapping;

    @Override
    public void execute(DelegateExecution execution) throws Exception {

        String tenantId = execution.getTenantId();
        String ticketId = String.valueOf(execution.getVariable("TicketID"));

        // ── 1. Resolve Expression fields (placeholders → actual values) ────────
        String resolvedAgentId       = resolvePlaceholders(resolveExpr(agentId,       execution), execution);
        String resolvedBaseUrl       = resolvePlaceholders(resolveExpr(baseUrl,       execution), execution);
        String resolvedRoute         = resolvePlaceholders(resolveExpr(route,         execution), execution);
        String resolvedAuthType      = resolvePlaceholders(resolveExpr(authType,      execution), execution);
        String resolvedInputParams   = resolvePlaceholders(resolveExpr(inputParams,   execution), execution);
        String resolvedOutputMapping = resolvePlaceholders(resolveExpr(outputMapping, execution), execution);

        log.info("=== OrchestratorAgentDelegate | agentId={} | TicketID={} ===",
                resolvedAgentId, ticketId);

        if (resolvedAgentId == null || resolvedAgentId.isEmpty()) {
            throw new BpmnError("AGENT_CONFIG_ERROR",
                    "OrchestratorAgentDelegate: agentId resolved to null/empty. " +
                            "Check that currentAgentId process variable is set by OrchestratorDelegate.");
        }

        // ── 2. Derive stage folder and run counter ─────────────────────────────
        // stageType set by OrchestratorDelegate as a process variable.
        // "execution-mode" → "Execution Mode", used as MinIO subfolder.
        String stageType   = getString(execution, "currentStageType");
        String stageFolder = toDisplayName(stageType != null ? stageType : "unknown");
        int    runCount    = incrementRunCount(execution, resolvedAgentId);
        log.info("Stage folder='{}' | run={}", stageFolder, runCount);

        Properties props        = TenantPropertiesUtil.getTenantProps(tenantId);
        StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);

        // ── 3. Build DIA request payload from inputParams ──────────────────────
        // inputParams tells us what to put in the "data" object.
        // Typically: [{"key":"context","type":"minioJson","value":"1/.../context.json","sourceJsonPath":"$"}]
        // which fetches context.json from MinIO and injects it as data.context.
        JSONObject dataObj = buildDataObject(resolvedInputParams, execution, storage);

        JSONObject requestBody = new JSONObject();
        requestBody.put("agentid", resolvedAgentId);
        requestBody.put("data",    dataObj);

        log.info("DIA payload built | agentId={} | data keys={}", resolvedAgentId, dataObj.keySet());

        // ── 4. Resolve DIA URL from base URL key ───────────────────────────────
        String propPrefix = "dia".equalsIgnoreCase(resolvedBaseUrl) ? DIA_PROP_KEY : resolvedBaseUrl;
        String baseUrlVal = props.getProperty(propPrefix + ".url");
        if (baseUrlVal == null || baseUrlVal.trim().isEmpty()) {
            throw new BpmnError("CONFIG_ERROR",
                    "OrchestratorAgentDelegate: Property '" + propPrefix + ".url' not found in tenant config.");
        }
        if (baseUrlVal.endsWith("/")) baseUrlVal = baseUrlVal.substring(0, baseUrlVal.length() - 1);

        String routePath = resolvedRoute != null ? resolvedRoute : "agent";
        if (routePath.startsWith("/")) routePath = routePath.substring(1);
        String fullUrl = baseUrlVal + "/" + routePath;

        // ── 5. Resolve credentials ─────────────────────────────────────────────
        String username = "";
        String password = "";
        if ("basicAuth".equalsIgnoreCase(resolvedAuthType)) {
            username = props.getProperty(propPrefix + ".username", "");
            password = props.getProperty(propPrefix + ".password", "");
        }

        log.info("Calling DIA: POST {}", fullUrl);

        // ── 6. Call DIA ────────────────────────────────────────────────────────
        String responseBody;
        try {
            responseBody = callDia(fullUrl, requestBody.toString(), username, password);
        } catch (Exception e) {
            log.error("DIA call failed for agentId={}: {}", resolvedAgentId, e.getMessage());
            JSONObject failResult = new JSONObject();
            failResult.put("action",  "FAIL");
            failResult.put("status",  "ERROR");
            failResult.put("message", e.getMessage());
            execution.setVariable("lastAgentResult", failResult.toString());
            return;
        }

        log.info("DIA response received | agentId={} | {} chars", resolvedAgentId, responseBody.length());

        // ── 7. Set lastAgentResult (always — OrchestratorDelegate reads this) ──
        execution.setVariable("lastAgentResult", responseBody);

        // ── 8. Process outputMapping ───────────────────────────────────────────
        // resolvedOutputMapping is the JSON array string resolved from ${currentOutputMapping}.
        // e.g. [{"type":"var","key":"agentAction","jsonPath":"$.action"},
        //        {"type":"minio","key":"execution-mode-agent_output","jsonPath":"$"}]
        JSONArray outputMappingArr;
        try {
            outputMappingArr = new JSONArray(
                    resolvedOutputMapping != null ? resolvedOutputMapping : "[]");
        } catch (Exception e) {
            log.warn("outputMapping is not a valid JSON array — skipping. Value: {}", resolvedOutputMapping);
            outputMappingArr = new JSONArray();
        }

        JSONObject agentResponse;
        try {
            agentResponse = new JSONObject(responseBody);
        } catch (Exception e) {
            log.warn("Agent response is not valid JSON — outputMapping skipped. Raw: {}", responseBody);
            return;
        }

        processOutputMapping(outputMappingArr, agentResponse, execution, storage,
                tenantId, ticketId, stageFolder, runCount);

        log.info("=== OrchestratorAgentDelegate Complete | agentId={} | run={} ===",
                resolvedAgentId, runCount);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // INPUT PARAMS — build the "data" object for the DIA payload
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Builds the "data" JSONObject to include in the DIA request body.
     *
     * Supported entry types in the inputParams JSON array:
     *
     * type="minioJson" — fetches a JSON file from MinIO, extracts sourceJsonPath,
     *                    injects the result under "key" in the data object.
     *   Example: {"key":"context","type":"minioJson",
     *             "value":"1/RuleCreation/2026001189/context.json","sourceJsonPath":"$"}
     *
     * type="value" — injects the literal "value" string under "key".
     *   Example: {"key":"tenantId","type":"value","value":"1"}
     *
     * Any ${...} placeholders in "value" are already resolved before this method is called.
     */
    private JSONObject buildDataObject(String inputParamsStr, DelegateExecution execution,
                                       StorageProvider storage) throws Exception {
        JSONObject dataObj = new JSONObject();

        if (inputParamsStr == null || inputParamsStr.trim().isEmpty()
                || inputParamsStr.trim().equals("[]")) {
            log.debug("inputParams is empty — data object will be empty");
            return dataObj;
        }

        JSONArray params;
        try {
            params = new JSONArray(inputParamsStr);
        } catch (Exception e) {
            log.warn("inputParams is not a valid JSON array — data object will be empty. Value: {}",
                    inputParamsStr);
            return dataObj;
        }

        for (int i = 0; i < params.length(); i++) {
            JSONObject entry = params.optJSONObject(i);
            if (entry == null) continue;

            String key            = entry.optString("key", "");
            String type           = entry.optString("type", "value");
            String val            = entry.optString("value", "");
            String sourceJsonPath = entry.optString("sourceJsonPath", "$");

            if (key.isEmpty()) {
                log.warn("inputParams entry {} has no 'key' — skipping", i);
                continue;
            }

            try {
                if ("minioJson".equalsIgnoreCase(type)) {
                    // Fetch JSON file from MinIO, extract sourceJsonPath, inject under key
                    log.debug("inputParams [minioJson]: key={} | path={} | jsonPath={}",
                            key, val, sourceJsonPath);
                    try (InputStream is = storage.downloadDocument(val)) {
                        String content = IOUtils.toString(is, StandardCharsets.UTF_8);
                        JSONObject fileJson = new JSONObject(content);
                        Object extracted = extractByJsonPath(fileJson, sourceJsonPath);
                        if (extracted instanceof JSONObject)      dataObj.put(key, (JSONObject) extracted);
                        else if (extracted instanceof JSONArray)  dataObj.put(key, (JSONArray)  extracted);
                        else dataObj.put(key, extracted != null ? extracted.toString() : JSONObject.NULL);
                    }
                    log.info("inputParams [minioJson]: key={} injected from {}", key, val);

                } else {
                    // type="value" — inject literal value
                    dataObj.put(key, val);
                    log.debug("inputParams [value]: key={} = {}", key, val);
                }

            } catch (Exception e) {
                log.warn("inputParams entry {}: failed to process key='{}' type='{}' — skipping. Reason: {}",
                        i, key, type, e.getMessage());
            }
        }

        return dataObj;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // OUTPUT MAPPING
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Iterates the outputMapping array and processes each entry.
     *
     * type="var"   → extracts value using jsonPath from agent response,
     *                sets as Camunda process variable under "key"
     * type="minio" → extracts value using jsonPath from agent response,
     *                saves as JSON to MinIO at:
     *                {tenantId}/RuleCreation/{ticketId}/{Stage Folder}/{key}_run{n}.json
     */
    private void processOutputMapping(JSONArray outputMapping, JSONObject agentResponse,
                                      DelegateExecution execution, StorageProvider storage,
                                      String tenantId, String ticketId,
                                      String stageFolder, int runCount) {

        for (int i = 0; i < outputMapping.length(); i++) {
            JSONObject entry = outputMapping.optJSONObject(i);
            if (entry == null) continue;

            String type     = entry.optString("type", "var");
            String key      = entry.optString("key", "");
            String jsonPath = entry.optString("jsonPath", "$");

            if (key.isEmpty()) {
                log.warn("outputMapping entry {} has no 'key' — skipping", i);
                continue;
            }

            try {
                Object extracted = extractByJsonPath(agentResponse, jsonPath);

                if ("minio".equalsIgnoreCase(type)) {
                    String minioPath = tenantId + "/" + WORKFLOW_KEY + "/" + ticketId
                            + "/" + stageFolder + "/" + key + "_run" + runCount + ".json";
                    String content = toJsonString(extracted);
                    storage.uploadDocument(minioPath,
                            content.getBytes(StandardCharsets.UTF_8), "application/json");
                    log.info("outputMapping [minio]: key={} → {}", key, minioPath);

                } else {
                    execution.setVariable(key, extracted != null ? extracted.toString() : null);
                    log.info("outputMapping [var]: key={} = {}", key,
                            extracted != null ? extracted.toString() : "null");
                }

            } catch (Exception e) {
                log.warn("outputMapping entry {}: failed to process key='{}' jsonPath='{}' — "
                        + "skipping. Reason: {}", i, key, jsonPath, e.getMessage());
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DIA HTTP CALL
    // ═══════════════════════════════════════════════════════════════════════════

    private String callDia(String url, String body, String username, String password)
            throws Exception {

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        try (CloseableHttpClient client = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build()) {

            HttpPost post = new HttpPost(url);
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Accept",       "application/json");
            post.setEntity(new StringEntity(body, StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = client.execute(post)) {
                int    statusCode    = response.getStatusLine().getStatusCode();
                String responseBody  = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

                log.info("DIA HTTP {}: {} chars", statusCode, responseBody.length());

                if (statusCode < 200 || statusCode >= 300) {
                    throw new RuntimeException("DIA returned HTTP " + statusCode + ": " + responseBody);
                }
                return responseBody;
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // HELPERS
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Reads an Expression field value as a String. Returns null if the field is null.
     */
    private String resolveExpr(Expression expr, DelegateExecution execution) {
        if (expr == null) return null;
        Object val = expr.getValue(execution);
        return val != null ? val.toString() : null;
    }

    /**
     * Resolves ${variableName} placeholders in a string by looking up the
     * named process variable in the current execution.
     * If the variable is not found, the placeholder is left as-is.
     */
    private String resolvePlaceholders(String input, DelegateExecution execution) {
        if (input == null) return null;
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(input);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String varName    = matcher.group(1).trim();
            Object val        = execution.getVariable(varName);
            String replacement = (val != null) ? val.toString() : "${" + varName + "}";
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * Converts a stageType slug into a human-readable folder name.
     * "execution-mode" → "Execution Mode", "product-class" → "Product Class"
     */
    private String toDisplayName(String stageType) {
        if (stageType == null || stageType.isEmpty()) return "Unknown";
        StringBuilder sb = new StringBuilder();
        for (String part : stageType.split("-")) {
            if (!part.isEmpty()) {
                sb.append(Character.toUpperCase(part.charAt(0)))
                        .append(part.substring(1))
                        .append(" ");
            }
        }
        return sb.toString().trim();
    }

    /**
     * Reads {agentId}_runCount from process variables, increments, saves back, returns new value.
     */
    private int incrementRunCount(DelegateExecution execution, String agentId) {
        String varName  = agentId + "_runCount";
        Object existing = execution.getVariable(varName);
        int    newCount = (existing != null) ? (Integer.parseInt(existing.toString()) + 1) : 1;
        execution.setVariable(varName, newCount);
        return newCount;
    }

    /**
     * Extracts a value from a JSONObject using a simple jsonPath expression.
     * Supports: "$" (whole object), "$.field", "$.nested.field" (one level).
     */
    private Object extractByJsonPath(JSONObject source, String jsonPath) {
        if ("$".equals(jsonPath)) return source;
        String path = jsonPath.startsWith("$.") ? jsonPath.substring(2) : jsonPath;
        if (path.contains(".")) {
            String[] parts = path.split("\\.", 2);
            Object nested  = source.opt(parts[0]);
            if (nested instanceof JSONObject) return ((JSONObject) nested).opt(parts[1]);
            return null;
        }
        return source.opt(path);
    }

    private String toJsonString(Object val) {
        if (val == null)                return "null";
        if (val instanceof JSONObject)  return ((JSONObject) val).toString(2);
        if (val instanceof JSONArray)   return ((JSONArray)  val).toString(2);
        return val.toString();
    }

    private String getString(DelegateExecution execution, String name) {
        Object val = execution.getVariable(name);
        return val != null ? val.toString() : null;
    }
}