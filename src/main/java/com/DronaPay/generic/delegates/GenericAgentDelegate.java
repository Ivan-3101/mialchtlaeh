package com.DronaPay.generic.delegates;

import com.DronaPay.generic.services.AgentResultStorageService;
import com.DronaPay.generic.services.ObjectStorageService;
import com.DronaPay.generic.storage.StorageProvider;
import com.DronaPay.generic.utils.TenantPropertiesUtil;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Generic Agent Delegate that executes external API calls based on a BPMN Element Template.
 *
 * It handles:
 * 1. Dynamic URL construction (Base URL + Route)
 * 2. Authentication (Basic Auth)
 * 3. Input payload construction (including file downloads from MinIO)
 * 4. Storing results back to MinIO and mapping outputs via a unified outputMapping config.
 *
 * Output options:
 *   3a. Always stores full result as MinIO JSON (default, always happens)
 *   3b. outputMapping (unified) — single JSON config that handles ALL of:
 *         PATTERN A — extract single path  → Camunda process variable
 *         PATTERN B — extract single path  → new MinIO file
 *         PATTERN C — merge multiple paths → Camunda process variable
 *         PATTERN D — merge multiple paths → new MinIO file
 *
 *   mergePaths sourceType options:
 *         "responseJson"    — (DEFAULT) current agent response JSON
 *         "processVariable" — a Camunda process variable (string, JSON string, or object)
 *         "minioFile"       — an existing MinIO file (via sourcePath or variableName)
 *
 * Run Count Tracking:
 *   On every execution, this delegate increments a per-agent counter stored as a
 *   process variable named "{agentId}_runCount". The current value is also exposed
 *   as "currentRunCount" so outputMapping targetPath values can reference
 *   ##currentRunCount## to version MinIO output files per run.
 *   This is used by the RuleCreation orchestrator loop. HealthClaim is unaffected
 *   because its outputMapping targetPaths are hardcoded and do not reference these variables.
 *
 * Placeholder resolution:
 *   resolvePlaceholders() handles two delimiter styles:
 *   - ##varName## — resolved FIRST. Used in camunda:inputParameter values where Camunda's
 *                   JUEL would eagerly evaluate ${...} at task entry before the delegate runs,
 *                   causing "Cannot resolve identifier" errors for variables not yet set
 *                   (e.g. ##contextMinioPath##, ##currentAgentId##, ##currentRunCount##).
 *   - ${varName}  — resolved SECOND. Standard process variable interpolation for variables
 *                   that exist at task entry (e.g. ${TicketID}, ${tenantId}).
 *
 * minioJson input type:
 *   Downloads a file directly from MinIO and parses it as JSON. Supports both:
 *   - Plain JSON files (e.g. context.json from the RuleCreation orchestrator loop)
 *   - AgentResultStorageService-wrapped files (which have a "rawResponse" key)
 *   If the file has a "rawResponse" field, its content is unwrapped and used as the JSON source.
 *   Otherwise, the raw file content is used directly. This replaces the previous
 *   AgentResultStorageService.retrieveAgentResult() call which only supported wrapped files.
 */
@Slf4j
public class GenericAgentDelegate implements JavaDelegate {

    // Fields injected from the BPMN Element Template
    private Expression baseUrl;
    private Expression route;
    private Expression authType;
    private Expression method;
    private Expression agentId;
    private Expression inputParams;
    private Expression outputMapping;

    @Override
    public void execute(DelegateExecution execution) throws Exception {

        // 1. Initialize Context
        String tenantId = execution.getTenantId();
        String rawAgentId = (agentId != null) ? (String) agentId.getValue(execution) : "unknown_agent";
        String currentAgentId = resolvePlaceholders(rawAgentId, execution, null);
        String stageName = execution.getCurrentActivityId();

        Object ticketIdObj = execution.getVariable("TicketID");
        String ticketId = (ticketIdObj != null) ? String.valueOf(ticketIdObj) : "UNKNOWN";

        log.info("=== Generic Agent Delegate Started: {} (Stage: {}) TicketID: {} ===",
                currentAgentId, stageName, ticketId);

        // ── Run count tracking ─────────────────────────────────────────────────
        // Increments a per-agent counter each time this agent is executed.
        // Exposed as `currentRunCount` so outputMapping targetPath values can
        // reference ##currentRunCount## to version MinIO output files per run.
        // Key is scoped to the agent ID so parallel agents don't collide.
        // HealthClaim is unaffected — its outputMapping targetPaths are hardcoded
        // strings that do not reference ##currentRunCount##.
        String runCountKey   = currentAgentId + "_runCount";
        Object existingCount = execution.getVariable(runCountKey);
        int currentRunCount  = (existingCount instanceof Integer) ? (Integer) existingCount : 0;
        currentRunCount++;
        execution.setVariable(runCountKey, currentRunCount);
        execution.setVariable("currentRunCount", currentRunCount);
        log.info("Run count | agentId={} | run={}", currentAgentId, currentRunCount);

        Properties props = TenantPropertiesUtil.getTenantProps(tenantId);

        // 2. Validate Required Configuration
        if (baseUrl == null || route == null) {
            throw new BpmnError("CONFIG_ERROR",
                    "Missing required fields: 'baseUrl' and 'route'. Ensure the BPMN task uses the correct template.");
        }

        String baseUrlKey = (String) baseUrl.getValue(execution);
        String routePath  = (String) route.getValue(execution);
        String authMethod = (authType != null) ? (String) authType.getValue(execution) : "none";

        // 3. Determine Property Prefix
        String propPrefix = baseUrlKey;
        if ("dia".equalsIgnoreCase(baseUrlKey)) {
            propPrefix = "agent.api";
        }

        // 4. Construct Full API URL
        String resolvedBaseUrl = props.getProperty(propPrefix + ".url");
        if (resolvedBaseUrl == null) {
            throw new BpmnError("CONFIG_ERROR", "Property not found: " + propPrefix + ".url");
        }
        if (resolvedBaseUrl.endsWith("/")) {
            resolvedBaseUrl = resolvedBaseUrl.substring(0, resolvedBaseUrl.length() - 1);
        }
        if (routePath.startsWith("/")) {
            routePath = routePath.substring(1);
        }
        String finalUrl = resolvedBaseUrl + "/" + routePath;

        // 5. Configure Authentication
        String apiUser = null;
        String apiPass = null;
        if ("basicAuth".equalsIgnoreCase(authMethod)) {
            apiUser = props.getProperty(propPrefix + ".username");
            apiPass = props.getProperty(propPrefix + ".password");
            if (apiUser == null) apiUser = props.getProperty("ai.agent.username");
            if (apiPass == null) apiPass = props.getProperty("ai.agent.password");
            if (apiUser == null || apiPass == null) {
                log.warn("Auth enabled but credentials missing for prefix: {}", propPrefix);
            }
        }

        String reqMethod = (method != null) ? (String) method.getValue(execution) : "POST";

        // 6. Build Request Payload
        String inputJsonStr      = (inputParams != null) ? (String) inputParams.getValue(execution) : "[]";
        String resolvedInputJson = resolvePlaceholders(inputJsonStr, execution, props);
        JSONObject dataObject    = buildDataObject(resolvedInputJson, tenantId);

        JSONObject requestBody = new JSONObject();
        requestBody.put("agentid", currentAgentId);
        requestBody.put("data", dataObject);

        // 7. Execute External API Call
        log.info("Calling Agent API: {} [{}]", finalUrl, reqMethod);
        String responseBody = executeAgentCall(reqMethod, finalUrl, apiUser, apiPass, requestBody.toString());

        // 8. Store Full Result in MinIO (3a — always happens)
        Map<String, Object> resultMap = AgentResultStorageService.buildResultMap(
                currentAgentId, 200, responseBody, new HashMap<>());

        Object attachmentObj  = execution.getVariable("attachment");
        String filename       = (attachmentObj != null) ? attachmentObj.toString() : currentAgentId + "_result";
        String resultFileName = (filename != null && !filename.isEmpty())
                ? filename : currentAgentId + "_result_" + System.currentTimeMillis();

        String minioPath = AgentResultStorageService.storeAgentResult(
                tenantId, ticketId, stageName, resultFileName, resultMap);

        execution.setVariable(currentAgentId + "_MinioPath", minioPath);
        log.info("Agent Result stored: {}", minioPath);

        // Build parsed full result JSON for outputMapping.
        // rawResponse in resultMap is an escaped JSON string — parse it into a real
        // JSONObject so that paths like $.rawResponse.answer work correctly.
        JSONObject resultJson = new JSONObject(resultMap);
        if (resultJson.has("rawResponse")) {
            try {
                String raw = resultJson.getString("rawResponse");
                resultJson.put("rawResponse", new JSONObject(raw));
            } catch (Exception e) {
                log.warn("rawResponse is not valid JSON for agent '{}', keeping as string", currentAgentId);
            }
        }
        String fullResultJson = resultJson.toString();

        // 9. Unified Output Mapping (3b)
        String outputMapStr = (outputMapping != null) ? (String) outputMapping.getValue(execution) : "{}";
        String resolvedOutputMapStr = resolvePlaceholders(outputMapStr, execution, props);
        processOutputMapping(fullResultJson, resolvedOutputMapStr, tenantId, execution, props);

        log.info("=== Generic Agent Delegate Completed ===");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // UNIFIED OUTPUT MAPPING
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Unified output mapping processor supporting 4 patterns.
     *
     * Descriptor fields:
     * ─────────────────────────────────────────────────────────────────────────
     *  storeIn        (required)  "processVariable" | "objectStorage"
     *  dataType       (optional)  "string" | "json"                    default: "json"
     *  path           (optional)  JsonPath into source JSON             default: "$"
     *                             (ignored when mergeVariables = true)
     *
     *  ── For storeIn = "objectStorage" ───────────────────────────────────────
     *  storageType    (optional)  "minio"                              default: "minio"
     *  targetPath     (required)  MinIO destination path. Supports ${var} and ##var## placeholders.
     *  targetVarName  (optional)  Process variable receiving the resolved MinIO path.
     *                             Defaults to "{outputKey}_minioPath".
     *
     *  ── For mergeVariables = true (Patterns C / D) ───────────────────────────
     *  mergeVariables (optional)  true → combine multiple sources into one object.
     *  mergePaths     (required when mergeVariables=true) Array of source descriptors:
     *    keyName      (required)  Key in the merged output object.
     *    sourceType   (optional)  "responseJson" (default) | "processVariable" | "minioFile"
     *    path         (optional)  JsonPath for responseJson / minioFile sources.
     *    variableName (optional)  Camunda variable name (for processVariable / minioFile).
     *    sourcePath   (optional)  Explicit MinIO path (for minioFile, supports ${var}).
     *    dataType     (optional)  "string" | "json".
     * ─────────────────────────────────────────────────────────────────────────
     *
     * PATTERN A — single path → process variable:
     *   "isForged": { "storeIn": "processVariable", "dataType": "string", "path": "$.rawResponse.answer" }
     *
     * PATTERN B — single path → MinIO file:
     *   "forgeryArchive": { "storeIn": "objectStorage", "path": "$.rawResponse",
     *                       "targetPath": "1/HealthClaim/${TicketID}/stage/${attachment}.json",
     *                       "targetVarName": "forgeryArchiveMinioPath" }
     *
     * PATTERN C — merged paths → process variable:
     *   "forgerySummary": { "storeIn": "processVariable", "mergeVariables": true,
     *                       "mergePaths": [
     *                         { "keyName": "answer",     "sourceType": "responseJson",    "path": "$.rawResponse.answer" },
     *                         { "keyName": "holderName", "sourceType": "processVariable", "variableName": "holder_name" },
     *                         { "keyName": "vpa",        "sourceType": "processVariable", "variableName": "vpaAttribs" },
     *                         { "keyName": "doc1",       "sourceType": "minioFile",       "sourcePath": "1/HealthClaim/${TicketID}/stage/Doc1.pdf.json", "path": "$.rawResponse.answer" }
     *                       ] }
     *
     * PATTERN D — merged paths → MinIO file:
     *   "mergedOutput": { "storeIn": "objectStorage", "mergeVariables": true,
     *                     "targetPath": "...", "targetVarName": "...",
     *                     "mergePaths": [ ... same entry formats as Pattern C ... ] }
     *
     * Available root keys in fullResultJson (responseJson source):
     *   agentId, statusCode, success, rawResponse (parsed object), extractedData, timestamp
     */
    private void processOutputMapping(String fullResultJson, String mappingStr,
                                      String tenantId, DelegateExecution execution, Properties props) {
        if (mappingStr == null || mappingStr.trim().equals("{}") || mappingStr.trim().isEmpty()) {
            log.debug("outputMapping is empty — skipping.");
            return;
        }

        JSONObject mapping;
        try {
            mapping = new JSONObject(mappingStr);
        } catch (Exception e) {
            log.error("outputMapping is not valid JSON — skipping. Value: {}", mappingStr);
            return;
        }

        Configuration jacksonConfig = Configuration.builder()
                .jsonProvider(new JacksonJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .build();

        com.jayway.jsonpath.DocumentContext responseDocumentContext =
                JsonPath.using(jacksonConfig).parse(fullResultJson);

        for (String outputKey : mapping.keySet()) {
            try {
                JSONObject descriptor = mapping.getJSONObject(outputKey);

                String  storeIn        = descriptor.optString("storeIn", "processVariable");
                boolean mergeVariables = descriptor.optBoolean("mergeVariables", false);
                String  dataType       = descriptor.optString("dataType", "json");

                if (mergeVariables) {
                    // ── PATTERNS C / D: merge multiple sources ────────────────────────
                    JSONArray mergePaths = descriptor.optJSONArray("mergePaths");
                    if (mergePaths == null || mergePaths.length() == 0) {
                        log.warn("outputMapping key '{}': mergeVariables=true but mergePaths is empty — skipping.", outputKey);
                        continue;
                    }

                    JSONObject merged = new JSONObject();

                    for (int i = 0; i < mergePaths.length(); i++) {
                        JSONObject mp = mergePaths.getJSONObject(i);

                        String keyName    = mp.optString("keyName", "");
                        String sourceType = mp.optString("sourceType", "responseJson");
                        String mergeType  = mp.optString("dataType", "json");
                        String mergePath  = mp.optString("path", "$");

                        if (keyName.isEmpty()) {
                            log.warn("outputMapping key '{}' mergePaths[{}]: missing keyName — skipping.",
                                    outputKey, i);
                            continue;
                        }

                        Object val = null;

                        try {
                            if ("processVariable".equalsIgnoreCase(sourceType)) {
                                // ── SOURCE: Camunda process variable ──────────────────────
                                String variableName = mp.optString("variableName", "");
                                if (variableName.isEmpty()) {
                                    log.warn("Merge [{}] entry {}: sourceType=processVariable but " +
                                            "variableName is empty — skipping.", outputKey, i);
                                    continue;
                                }
                                Object rawVal = execution.getVariable(variableName);
                                if (rawVal == null) {
                                    log.warn("Merge [{}] entry {}: processVariable '{}' is null — skipping.",
                                            outputKey, i, variableName);
                                    continue;
                                }
                                if (rawVal instanceof String) {
                                    String strVal = ((String) rawVal).trim();
                                    if (strVal.startsWith("{")) {
                                        try {
                                            val = new JSONObject(strVal);
                                        } catch (Exception ignored) {
                                            val = strVal;
                                        }
                                    } else if (strVal.startsWith("[")) {
                                        try {
                                            val = new JSONArray(strVal);
                                        } catch (Exception ignored) {
                                            val = strVal;
                                        }
                                    } else {
                                        val = strVal;
                                    }
                                } else {
                                    val = rawVal;
                                }
                                log.debug("Merge [{}]: keyName='{}' sourceType=processVariable " +
                                                "variableName='{}' type={}",
                                        outputKey, keyName, variableName, val.getClass().getSimpleName());

                            } else if ("minioFile".equalsIgnoreCase(sourceType)) {
                                // ── SOURCE: Existing MinIO file ───────────────────────────
                                String resolvedMinioPath = resolveMinioSourcePath(
                                        mp, execution, props, outputKey, i);
                                if (resolvedMinioPath == null) continue;

                                String fileContent = downloadMinioFileAsString(
                                        resolvedMinioPath, tenantId, outputKey, i);
                                if (fileContent == null) continue;

                                val = extractFromJson(
                                        fileContent, mergePath, mergeType, jacksonConfig, outputKey, keyName);
                                if (val == null) continue;

                                log.debug("Merge [{}]: keyName='{}' sourceType=minioFile " +
                                        "path='{}' from '{}'", outputKey, keyName, mergePath, resolvedMinioPath);

                            } else {
                                // ── SOURCE: responseJson (DEFAULT) ────────────────────────
                                val = responseDocumentContext.read(mergePath);
                                val = coerceType(val, mergeType);
                                log.debug("Merge [{}]: keyName='{}' sourceType=responseJson " +
                                        "path='{}' → '{}'", outputKey, keyName, mergePath, val);
                            }

                        } catch (Exception e) {
                            log.warn("Merge [{}] entry {}: error resolving keyName='{}' — " +
                                    "skipping entry. Reason: {}", outputKey, i, keyName, e.getMessage());
                            continue;
                        }

                        // Add to merged object.
                        // Duplicate keyNames collect into a JSON Array.
                        if (merged.has(keyName)) {
                            Object existing = merged.get(keyName);
                            JSONArray arr = new JSONArray();
                            addToArray(arr, existing);
                            addToArray(arr, val);
                            merged.put(keyName, arr);
                        } else {
                            putJsonValue(merged, keyName, val);
                        }
                    }

                    // Route merged object to destination
                    if ("objectStorage".equalsIgnoreCase(storeIn)) {
                        // ── PATTERN D: Upload merged object as a MinIO file ───────────────
                        String targetPath    = descriptor.optString("targetPath", "");
                        String targetVarName = descriptor.optString("targetVarName", outputKey + "_minioPath");

                        if (targetPath.isEmpty()) {
                            log.warn("outputMapping key '{}': storeIn=objectStorage + mergeVariables=true " +
                                    "but targetPath is empty — skipping", outputKey);
                            continue;
                        }

                        String serialized         = merged.toString(2);
                        String resolvedTargetPath = resolvePlaceholders(targetPath, execution, props);
                        StorageProvider storage   = ObjectStorageService.getStorageProvider(tenantId);
                        storage.uploadDocument(resolvedTargetPath,
                                serialized.getBytes(StandardCharsets.UTF_8), "application/json");
                        execution.setVariable(targetVarName, resolvedTargetPath);
                        log.info("Output mapping (merge/objectStorage): merged {} paths → " +
                                "MinIO='{}' → var='{}'", mergePaths.length(), resolvedTargetPath, targetVarName);

                    } else {
                        // ── PATTERN C: Set merged object as Camunda process variable ──────
                        execution.setVariable(outputKey, merged.toString());
                        log.info("Output mapping (merge/processVariable): set '{}' as merged " +
                                "JSON object ({} paths)", outputKey, mergePaths.length());
                    }

                } else {
                    // ── PATTERN A & B ─────────────────────────────────────────────────────
                    // Unchanged — reads from current agent response only.
                    String path = descriptor.optString("path", "$");
                    Object val;
                    try {
                        val = responseDocumentContext.read(path);
                    } catch (Exception e) {
                        log.warn("outputMapping key '{}': could not extract path '{}' — skipping",
                                outputKey, path);
                        continue;
                    }

                    val = coerceType(val, dataType);

                    if ("objectStorage".equalsIgnoreCase(storeIn)) {
                        // ── PATTERN B ──────────────────────────────────────────────────────
                        String targetPath    = descriptor.optString("targetPath", "");
                        String targetVarName = descriptor.optString("targetVarName", outputKey + "_minioPath");

                        if (targetPath.isEmpty()) {
                            log.warn("outputMapping key '{}': storeIn=objectStorage but " +
                                    "targetPath is empty — skipping", outputKey);
                            continue;
                        }

                        String serialized;
                        if (val instanceof Map) {
                            serialized = new JSONObject((Map<?, ?>) val).toString(2);
                        } else if (val instanceof List) {
                            serialized = new JSONArray((List<?>) val).toString(2);
                        } else {
                            serialized = (val != null) ? val.toString() : "null";
                        }

                        String resolvedTargetPath = resolvePlaceholders(targetPath, execution, props);
                        StorageProvider storage   = ObjectStorageService.getStorageProvider(tenantId);
                        storage.uploadDocument(resolvedTargetPath,
                                serialized.getBytes(StandardCharsets.UTF_8), "application/json");
                        execution.setVariable(targetVarName, resolvedTargetPath);
                        log.info("Output mapping (objectStorage): path='{}' → MinIO='{}' → var='{}'",
                                path, resolvedTargetPath, targetVarName);

                    } else {
                        // ── PATTERN A ──────────────────────────────────────────────────────
                        execution.setVariable(outputKey, val);
                        log.info("Output mapping (processVariable): set '{}' from path '{}' = '{}'",
                                outputKey, path, val);
                    }
                }

            } catch (Exception e) {
                log.error("outputMapping key '{}': unexpected error — {}", outputKey, e.getMessage(), e);
                throw new BpmnError("OUTPUT_MAPPING_ERROR",
                        "Failed to process output mapping key '" + outputKey + "': " + e.getMessage());
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HELPER METHODS
    // ─────────────────────────────────────────────────────────────────────────

    private JSONObject buildDataObject(String inputJsonStr, String tenantId) throws Exception {
        JSONObject data = new JSONObject();
        JSONArray inputs;
        try {
            inputs = new JSONArray(inputJsonStr);
        } catch (Exception e) {
            log.warn("inputParams is not a valid JSON array — data object will be empty. Value: {}", inputJsonStr);
            return data;
        }

        StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);

        for (int i = 0; i < inputs.length(); i++) {
            JSONObject input = inputs.getJSONObject(i);
            String key   = input.getString("key");
            String type  = input.optString("type", "value");
            String value = input.optString("value", "");

            if (value.isEmpty()) continue;

            if ("minioFile".equalsIgnoreCase(type)) {
                try {
                    InputStream fileContent = storage.downloadDocument(value);
                    byte[] bytes = IOUtils.toByteArray(fileContent);
                    data.put(key, Base64.getEncoder().encodeToString(bytes));
                } catch (Exception e) {
                    log.error("Failed to download file from MinIO: {}", value, e);
                    throw new BpmnError("FILE_ERROR", "Could not process file: " + value);
                }
            } else if ("minioJson".equalsIgnoreCase(type)) {
                // Downloads a file directly from MinIO and parses it as JSON.
                //
                // Supports two file formats:
                //   1. Plain JSON files (e.g. context.json from the RuleCreation orchestrator loop)
                //      → used directly as the JSON source
                //   2. AgentResultStorageService-wrapped files (HealthClaim agent output files)
                //      → have a "rawResponse" key; its content is unwrapped and used as the JSON source
                //
                // This replaces the previous AgentResultStorageService.retrieveAgentResult() call
                // which only supported wrapped files and failed on plain JSON files like context.json.
                try {
                    InputStream stream = storage.downloadDocument(value);
                    String rawContent  = IOUtils.toString(stream, StandardCharsets.UTF_8);

                    // Normalise: if wrapped in AgentResultStorageService format, unwrap rawResponse
                    String jsonContent = rawContent;
                    try {
                        JSONObject wrapper = new JSONObject(rawContent);
                        if (wrapper.has("rawResponse")) {
                            Object rawResp = wrapper.get("rawResponse");
                            jsonContent = (rawResp instanceof String)
                                    ? (String) rawResp : rawResp.toString();
                            log.debug("minioJson [{}]: unwrapped rawResponse from AgentResultStorageService format", key);
                        }
                    } catch (Exception ignored) {
                        // Not a JSON object wrapper — use rawContent directly
                    }

                    String sourcePath = input.optString("sourceJsonPath", "");
                    if (!sourcePath.isEmpty()) {
                        Object extracted = JsonPath.read(jsonContent, sourcePath);
                        if (extracted instanceof Map) {
                            data.put(key, new JSONObject((Map<?, ?>) extracted));
                        } else if (extracted instanceof List) {
                            data.put(key, new JSONArray((List<?>) extracted));
                        } else {
                            data.put(key, extracted);
                        }
                    } else {
                        data.put(key, new JSONObject(jsonContent));
                    }
                    log.debug("minioJson [{}]: loaded from '{}' sourceJsonPath='{}'", key, value,
                            input.optString("sourceJsonPath", "$"));
                } catch (Exception e) {
                    log.error("Failed to process MinIO JSON '{}': {}", value, e);
                    throw new BpmnError("FILE_ERROR", "Could not process JSON: " + value);
                }
            } else {
                // type="value" — inject literal value
                data.put(key, value);
            }
        }
        return data;
    }

    private String executeAgentCall(String method, String url,
                                    String user, String pass, String body) throws Exception {
        CredentialsProvider provider = new BasicCredentialsProvider();
        if (user != null && pass != null) {
            provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, pass));
        }
        try (CloseableHttpClient client = HttpClients.custom()
                .setDefaultCredentialsProvider(provider).build()) {
            HttpRequestBase request;
            if ("GET".equalsIgnoreCase(method)) {
                request = new HttpGet(url);
            } else {
                HttpPost post = new HttpPost(url);
                post.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
                post.setHeader("Content-Type", "application/json");
                request = post;
            }
            try (CloseableHttpResponse response = client.execute(request)) {
                int status     = response.getStatusLine().getStatusCode();
                String respStr = EntityUtils.toString(response.getEntity());
                if (status != 200) {
                    throw new BpmnError("AGENT_ERROR", "Agent returned " + status + ": " + respStr);
                }
                return respStr;
            }
        }
    }

    /**
     * Resolves placeholders in two passes:
     *
     * Pass 1 — ##varName## delimiters:
     *   Used in camunda:inputParameter values where Camunda JUEL eagerly evaluates ${...}
     *   at task entry — before the delegate runs. Variables like currentAgentId, currentRunCount,
     *   and contextMinioPath are not yet set at that point, causing "Cannot resolve identifier"
     *   errors. Using ##...## bypasses JUEL and lets this delegate resolve them at runtime.
     *
     * Pass 2 — ${varName} delimiters:
     *   Standard resolution for variables that exist at task entry (e.g. TicketID, tenantId).
     *   Also handles appproperties.* and map access with [varName] syntax.
     */
    private String resolvePlaceholders(String input, DelegateExecution execution, Properties props) {
        if (input == null) return null;

        // Pass 1: resolve ##varName## — deferred placeholders safe from JUEL eager evaluation
        Pattern hashPattern = Pattern.compile("##([^#]+)##");
        Matcher hashMatcher = hashPattern.matcher(input);
        StringBuffer hashSb = new StringBuffer();
        while (hashMatcher.find()) {
            String varName = hashMatcher.group(1).trim();
            Object val = execution.getVariable(varName);
            String replacement = (val != null) ? val.toString() : "##" + varName + "##";
            hashMatcher.appendReplacement(hashSb, Matcher.quoteReplacement(replacement));
        }
        hashMatcher.appendTail(hashSb);
        input = hashSb.toString();

        // Pass 2: resolve ${...} — standard process variable interpolation
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(input);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String token = matcher.group(1).trim();
            String replacement = "";
            if (token.startsWith("appproperties.") && props != null) {
                replacement = props.getProperty(token.substring("appproperties.".length()), "");
            } else if (token.contains("[") && token.endsWith("]")) {
                replacement = resolveMapValue(token, execution);
            } else {
                String varName = token.startsWith("processVariable.")
                        ? token.substring("processVariable.".length()) : token;
                Object val = execution.getVariable(varName);
                replacement = (val != null) ? val.toString() : "${" + token + "}";
            }
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private String resolveMapValue(String token, DelegateExecution execution) {
        try {
            int bracketIndex   = token.indexOf("[");
            String mapName     = token.substring(0, bracketIndex);
            String keyVarName  = token.substring(bracketIndex + 1, token.length() - 1);
            Object mapObj      = execution.getVariable(mapName);
            Object keyValObj   = execution.getVariable(keyVarName);
            String resolvedKey = (keyValObj != null) ? keyValObj.toString() : keyVarName;
            if (mapObj instanceof Map) {
                Object result = ((Map<?, ?>) mapObj).get(resolvedKey);
                return result != null ? result.toString() : "";
            }
        } catch (Exception e) {
            log.warn("Could not resolve map value for token: {}", token);
        }
        return "";
    }

    /**
     * Resolves the MinIO file path for a sourceType=minioFile mergePaths entry.
     *
     * Priority:
     *   1. "sourcePath" — explicit path, supports ${var} placeholders
     *      e.g. "1/HealthClaim/${TicketID}/Identify_Forged_Documents/Doc1.pdf.json"
     *   2. "variableName" — name of a process variable holding the MinIO path
     *      e.g. "verifyResponse_minioPath"
     *
     * Returns null if neither is provided or the variable is empty (warning logged).
     */
    private String resolveMinioSourcePath(JSONObject mp, DelegateExecution execution,
                                          Properties props, String outputKey, int entryIndex) {
        String sourcePath   = mp.optString("sourcePath", "").trim();
        String variableName = mp.optString("variableName", "").trim();

        if (!sourcePath.isEmpty()) {
            String resolved = resolvePlaceholders(sourcePath, execution, props);
            log.debug("Merge [{}] entry {}: minioFile sourcePath resolved to '{}'",
                    outputKey, entryIndex, resolved);
            return resolved;
        }

        if (!variableName.isEmpty()) {
            Object pathVar = execution.getVariable(variableName);
            if (pathVar == null) {
                log.warn("Merge [{}] entry {}: sourceType=minioFile variableName='{}' is null " +
                        "or not set — skipping", outputKey, entryIndex, variableName);
                return null;
            }
            String resolved = pathVar.toString().trim();
            log.debug("Merge [{}] entry {}: minioFile variableName='{}' resolved to path '{}'",
                    outputKey, entryIndex, variableName, resolved);
            return resolved;
        }

        log.warn("Merge [{}] entry {}: sourceType=minioFile but neither sourcePath nor " +
                "variableName provided — skipping", outputKey, entryIndex);
        return null;
    }

    /**
     * Downloads a MinIO file and returns its content as a normalized JSON string.
     *
     * FIX: If the downloaded file contains a "rawResponse" field stored as an escaped
     * JSON string (which is how AgentResultStorageService stores agent results), it is
     * automatically parsed into a real nested object before returning. This ensures that
     * JsonPath expressions like $.rawResponse.answer work correctly on downloaded files,
     * exactly the same way they work on the live agent response in execute().
     *
     * Returns null and logs a warning if the download fails.
     */
    private String downloadMinioFileAsString(String minioPath, String tenantId,
                                             String outputKey, int entryIndex) {
        try {
            StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);
            InputStream stream      = storage.downloadDocument(minioPath);
            String raw              = new String(IOUtils.toByteArray(stream), StandardCharsets.UTF_8);

            // Normalize rawResponse escaped string → nested object.
            // Agent result files store rawResponse as an escaped JSON string:
            //   "rawResponse": "{\"answer\":{...}}"
            // This makes $.rawResponse.answer unreachable via JsonPath.
            // We parse it here so all paths work consistently with the live response.
            try {
                JSONObject fileJson = new JSONObject(raw);
                if (fileJson.has("rawResponse")) {
                    Object rawResp = fileJson.get("rawResponse");
                    if (rawResp instanceof String) {
                        String rawRespStr = ((String) rawResp).trim();
                        if (rawRespStr.startsWith("{")) {
                            fileJson.put("rawResponse", new JSONObject(rawRespStr));
                            raw = fileJson.toString();
                            log.debug("Merge [{}] entry {}: normalized rawResponse in '{}'",
                                    outputKey, entryIndex, minioPath);
                        }
                    }
                }
            } catch (Exception e) {
                log.debug("Merge [{}] entry {}: rawResponse normalization skipped for '{}': {}",
                        outputKey, entryIndex, minioPath, e.getMessage());
            }

            log.debug("Merge [{}] entry {}: downloaded MinIO file '{}' ({} bytes)",
                    outputKey, entryIndex, minioPath, raw.length());
            return raw;

        } catch (Exception e) {
            log.warn("Merge [{}] entry {}: failed to download MinIO file '{}' — skipping. " +
                    "Reason: {}", outputKey, entryIndex, minioPath, e.getMessage());
            return null;
        }
    }

    /**
     * Extracts a JsonPath value from a JSON string.
     * Returns null and logs a warning if extraction fails.
     */
    private Object extractFromJson(String jsonContent, String path, String dataType,
                                   Configuration jacksonConfig, String outputKey, String keyName) {
        try {
            var ctx    = JsonPath.using(jacksonConfig).parse(jsonContent);
            Object val = ctx.read(path);
            return coerceType(val, dataType);
        } catch (Exception e) {
            log.warn("Merge [{}]: keyName='{}' could not extract path '{}' from downloaded " +
                    "file — skipping. Reason: {}", outputKey, keyName, path, e.getMessage());
            return null;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TYPE COERCION
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Coerces an extracted JsonPath value to the declared dataType.
     *
     * "string" — calls toString() (Maps/Lists serialized to JSON string)
     * "json"   — keeps Maps and Lists as-is for proper nested serialization
     *
     * Note: NOT called for sourceType=processVariable entries — those handle
     * their own type resolution inline to avoid double-serialization issues.
     */
    private Object coerceType(Object val, String dataType) {
        if (val == null) return null;
        if ("string".equalsIgnoreCase(dataType)) {
            if (val instanceof Map) {
                return new JSONObject((Map<?, ?>) val).toString();
            } else if (val instanceof List) {
                return new JSONArray((List<?>) val).toString();
            }
            return val.toString();
        }
        return val;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TYPE-SAFE JSON HELPERS
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Type-safe put into a JSONObject.
     *
     * Explicitly dispatches by runtime type before calling the matching put() overload.
     * This avoids the "Ambiguous method call" compile error that occurs when passing
     * an Object to JSONObject.put() — the compiler cannot choose between overloads
     * like put(String, Map) and put(String, Collection) when the type is Object.
     */
    private void putJsonValue(JSONObject target, String key, Object val) throws Exception {
        if (val == null)                    target.put(key, JSONObject.NULL);
        else if (val instanceof JSONObject) target.put(key, (JSONObject) val);
        else if (val instanceof JSONArray)  target.put(key, (JSONArray) val);
        else if (val instanceof Map)        target.put(key, new JSONObject((Map<?, ?>) val));
        else if (val instanceof List)       target.put(key, new JSONArray((List<?>) val));
        else if (val instanceof Boolean)    target.put(key, (boolean) val);
        else if (val instanceof Integer)    target.put(key, (int) val);
        else if (val instanceof Long)       target.put(key, (long) val);
        else if (val instanceof Double)     target.put(key, (double) val);
        else                                target.put(key, val.toString());
    }

    /**
     * Type-safe add to a JSONArray.
     *
     * Same reason as putJsonValue — avoids ambiguous overload compile errors
     * when the value is typed as Object.
     */
    private void addToArray(JSONArray arr, Object val) throws Exception {
        if (val == null)                    arr.put(JSONObject.NULL);
        else if (val instanceof JSONObject) arr.put((JSONObject) val);
        else if (val instanceof JSONArray)  arr.put((JSONArray) val);
        else if (val instanceof Map)        arr.put(new JSONObject((Map<?, ?>) val));
        else if (val instanceof List)       arr.put(new JSONArray((List<?>) val));
        else if (val instanceof Boolean)    arr.put((boolean) val);
        else if (val instanceof Integer)    arr.put((int) val);
        else if (val instanceof Long)       arr.put((long) val);
        else if (val instanceof Double)     arr.put((double) val);
        else                                arr.put(val.toString());
    }
}