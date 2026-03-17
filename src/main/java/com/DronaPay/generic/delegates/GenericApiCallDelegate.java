package com.DronaPay.generic.delegates;

import com.DronaPay.generic.services.ObjectStorageService;
import com.DronaPay.generic.storage.StorageProvider;
import com.DronaPay.generic.utils.TenantPropertiesUtil;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.cibseven.bpm.engine.delegate.BpmnError;
import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.Expression;
import org.cibseven.bpm.engine.delegate.JavaDelegate;
import org.cibseven.bpm.engine.variable.Variables;
import org.cibseven.bpm.engine.variable.value.ObjectValue;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generic HTTP API Call Delegate.
 *
 * Makes a configurable REST call and processes its response using a unified
 * outputMapping config — identical in structure and capability to GenericAgentDelegate.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * INPUT BODY CONSTRUCTION via inputParams JSON Array
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * inputParams is a JSON Array. Each entry has a "type" field:
 *
 *   type: "value"
 *     - WITH a "key"    → adds {"key": resolvedValue} to the body JSON object.
 *                         resolvedValue goes through full placeholder resolution
 *                         (${fn.uuid}, ${fn.isoTimestamp}, ${processVariable.x}, etc.)
 *                         If resolvedValue is itself a valid JSON object/array string,
 *                         it is inlined as a nested object (not double-serialized).
 *     - WITHOUT a "key" → the entire "value" string IS the raw request body
 *                         (after placeholder resolution). Use this for passing a
 *                         large pre-formed JSON body (e.g. the FWA / CC payload).
 *
 *   type: "minioFile"
 *     - Downloads the file at the MinIO path given by "value" (supports ${} placeholders).
 *     - Base64-encodes the file bytes.
 *     - Adds {"key": "<base64string>"} to the body JSON object.
 *     - Requires "key".
 *
 *   type: "minioJson"
 *     - Downloads the file at the MinIO path given by "value" (supports ${} placeholders).
 *     - Parses the file as JSON.
 *     - Optionally extracts at "sourceJsonPath" (defaults to "$" = whole file).
 *     - Adds {"key": <extractedObject>} to the body JSON object.
 *     - Requires "key".
 *
 * Examples:
 *
 *   // Pass a large pre-formed JSON body (e.g. FWA / CC stage):
 *   [{"type": "value", "value": "{\"reqid\": \"${fn.uuid}\", \"org\": \"SIT\", ...}"}]
 *
 *   // Build body from individual keys:
 *   [
 *     {"key": "policy_id", "type": "value",    "value": "${processVariable.policy_id}"},
 *     {"key": "document",  "type": "minioFile", "value": "${documentPath}"},
 *     {"key": "attribs",   "type": "minioJson", "value": "${attribsMinioPath}", "sourceJsonPath": "$.attribs"}
 *   ]
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * OUTPUT options (unchanged):
 *   Step 8.  Always stores full response as a Camunda process variable ({outputVar}).
 *   Step 9.  Always stores full response as a MinIO file ({outputVar}_minioPath).
 *   Step 10. outputMapping (unified) — single JSON config that handles ALL of:
 *              PATTERN A — extract single path  → Camunda process variable
 *              PATTERN B — extract single path  → new MinIO file
 *              PATTERN C — merge multiple paths → Camunda process variable
 *              PATTERN D — merge multiple paths → new MinIO file
 *
 *   mergePaths sourceType options:
 *              "responseJson"    — (DEFAULT) the raw API response body
 *              "processVariable" — a Camunda process variable (string, JSON string, or object)
 *              "minioFile"       — an existing MinIO file (via sourcePath or variableName)
 *
 * outputMapping format examples:
 * ─────────────────────────────────────────────────────────────────────────────
 * PATTERN A — single path → process variable:
 *   "policyFound": { "storeIn": "processVariable", "dataType": "string", "path": "$.verified" }
 *
 * PATTERN B — single path → MinIO file:
 *   "attribsFile": { "storeIn": "objectStorage", "path": "$.attribs",
 *                    "targetPath": "1/${workflowKey}/${TicketID}/stage/attribs.json",
 *                    "targetVarName": "attribsMinioPath" }
 *
 * PATTERN C — merged paths → process variable:
 *   "summary": { "storeIn": "processVariable", "mergeVariables": true,
 *                "mergePaths": [
 *                  { "keyName": "holderName", "sourceType": "responseJson",    "path": "$.accountName" },
 *                  { "keyName": "riskScore",  "sourceType": "processVariable", "variableName": "RiskScore" },
 *                  { "keyName": "docData",    "sourceType": "minioFile",       "sourcePath": "1/${workflowKey}/${TicketID}/stage/doc.json", "path": "$.rawResponse" }
 *                ] }
 *
 * PATTERN D — merged paths → MinIO file:
 *   "report": { "storeIn": "objectStorage", "mergeVariables": true,
 *               "targetPath": "1/${workflowKey}/${TicketID}/stage/report.json",
 *               "targetVarName": "reportMinioPath",
 *               "mergePaths": [ ... same entry formats as Pattern C ... ] }
 *
 * Available root keys in responseJson source:
 *   All top-level keys from the raw API response body (e.g. $.verified, $.accountName, $.attribs)
 * ─────────────────────────────────────────────────────────────────────────────
 */
@Slf4j
public class GenericApiCallDelegate implements JavaDelegate {

    // INPUTS (Injected from the Camunda Element Template)
    private Expression baseUrl;       // Dropdown key → property prefix (e.g. "springapi" → "springapi.url")
    private Expression route;         // API route, supports ${processVariable.x} (e.g. "/accounts/${processVariable.policy_id}")
    private Expression authType;      // Auth method (e.g. "xApiKey")
    private Expression method;        // HTTP Method (GET/POST/PUT/DELETE)
    private Expression inputParams;   // JSON Array describing how to build the request body
    private Expression outputVar;     // Process variable name for full response. Also derives {outputVar}_minioPath.
    private Expression outputMapping; // Unified output config (Patterns A/B/C/D). Enter {} to skip.

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        log.info("=== Generic API Call Started ===");

        // 1. SETUP
        String tenantId = execution.getTenantId();
        if (tenantId == null || tenantId.isEmpty()) {
            throw new BpmnError("CONFIG_ERROR", "Tenant ID is missing from execution context.");
        }

        Properties props = TenantPropertiesUtil.getTenantProps(tenantId);

        // 2. READ FIELD INPUTS
        String baseUrlKey = baseUrl   != null ? (String) baseUrl.getValue(execution)   : "springapi";
        String routePath  = route     != null ? (String) route.getValue(execution)     : "/";
        String authMethod = authType  != null ? (String) authType.getValue(execution)  : "xApiKey";
        String reqMethod  = method    != null ? (String) method.getValue(execution)    : "GET";
        String targetVar  = outputVar != null ? (String) outputVar.getValue(execution) : "apiResponse";

        // 3. BUILD REQUEST BODY FROM inputParams
        String finalBody = buildRequestBody(execution, props, tenantId);

        // 4. CONSTRUCT URL
        String resolvedBase = props.getProperty(baseUrlKey + ".url");
        if (resolvedBase == null) {
            throw new BpmnError("CONFIG_ERROR", "Property not found: " + baseUrlKey + ".url");
        }

        if (resolvedBase.endsWith("/")) resolvedBase = resolvedBase.substring(0, resolvedBase.length() - 1);

        String resolvedRoute = resolvePlaceholders(routePath, execution, props);
        if (!resolvedRoute.startsWith("/")) resolvedRoute = "/" + resolvedRoute;

        String finalUrl = resolvedBase + resolvedRoute;
        log.info("Making {} request to: {}", reqMethod, finalUrl);
        log.info("Request body being sent: {}", finalBody);

        // 5. EXECUTE HTTP REQUEST
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpRequestBase request = createRequest(reqMethod, finalUrl, finalBody);

            // 6. AUTHENTICATION
            if ("xApiKey".equalsIgnoreCase(authMethod)) {
                String apiKey = props.getProperty(baseUrlKey + ".api.key");
                if (apiKey == null) {
                    throw new BpmnError("CONFIG_ERROR", "Property not found: " + baseUrlKey + ".api.key");
                }
                request.addHeader("x-api-key", apiKey);
            }

            request.addHeader("Content-Type", "application/json");

            try (CloseableHttpResponse response = client.execute(request)) {
                int statusCode = response.getStatusLine().getStatusCode();
                String responseBody = EntityUtils.toString(response.getEntity());

                log.info("API Response Status: {}", statusCode);

                // 7. FAIL LOGIC
                if (statusCode != 200) {
                    log.error("API Call Failed. Status: {}, Body: {}", statusCode, responseBody);
                    throw new BpmnError("API_ERROR", "API Call failed with status code: " + statusCode);
                }

                // 8. STORE FULL RESPONSE AS PROCESS VARIABLE (always happens)
                execution.setVariable(targetVar + "_statusCode", statusCode);
                try {
                    ObjectValue respJson = Variables.objectValue(responseBody)
                            .serializationDataFormat("application/json").create();
                    execution.setVariable(targetVar, respJson);
                } catch (Exception e) {
                    execution.setVariable(targetVar, responseBody);
                }

                // 9. STORE FULL RESPONSE IN MINIO (always happens)
                // Path: {tenantId}/{workflowKey}/{ticketId}/{stageName}/{outputVar}.json
                // Sets: {outputVar}_minioPath
                try {
                    String ticketId = execution.getVariable("TicketID") != null
                            ? String.valueOf(execution.getVariable("TicketID"))
                            : "UNKNOWN";
                    String workflowKey = execution.getProcessDefinitionId().split(":")[0];
                    String stageName = execution.getCurrentActivityId();

                    String minioPath = tenantId + "/" + workflowKey + "/" + ticketId + "/" + stageName + "/" + targetVar + ".json";

                    StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);
                    byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
                    storage.uploadDocument(minioPath, bytes, "application/json");

                    execution.setVariable(targetVar + "_minioPath", minioPath);
                    log.info("Response stored to MinIO: {} → var '{}'", minioPath, targetVar + "_minioPath");
                } catch (Exception e) {
                    log.error("Failed to store API response to MinIO: {}", e.getMessage());
                    throw new BpmnError("STORAGE_ERROR", "Failed to store API response to MinIO: " + e.getMessage());
                }

                // 10. UNIFIED OUTPUT MAPPING (Patterns A / B / C / D)
                String outputMappingStr = (outputMapping != null) ? (String) outputMapping.getValue(execution) : "{}";
                processOutputMapping(responseBody, outputMappingStr, tenantId, execution, props);
            }
        }

        log.info("=== Generic API Call Completed ===");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // INPUT BODY BUILDER
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Builds the final request body string from the inputParams field.
     *
     * THREE MODES depending on the content of inputParams:
     *
     *  MODE 1 — Raw JSON object (starts with '{'):
     *    The entire inputParams value IS the request body. Placeholder resolution is applied.
     *    This is the simplest mode — paste your JSON body directly, exactly as in the old 'body' field.
     *    Example:
     *      {
     *        "reqid": "${fn.uuid}",
     *        "org": "SIT",
     *        ...
     *      }
     *
     *  MODE 2 — JSON Array (starts with '['):
     *    Each entry in the array has a "type" field:
     *      type "value"    + no key  → entry's value IS the entire raw body (after placeholder resolution).
     *      type "value"    + key     → resolves value and adds to body JSON object under that key.
     *      type "minioFile"+ key     → downloads file, base64-encodes, adds to body object under key.
     *      type "minioJson"+ key     → downloads JSON file, extracts at sourceJsonPath, adds under key.
     *
     *  MODE 3 — Empty / null / "[]":
     *    No body is sent. Use this for GET requests.
     */
    private String buildRequestBody(DelegateExecution execution, Properties props, String tenantId) throws Exception {

        if (inputParams == null) {
            log.info("inputParams is null — no request body.");
            return null;
        }

        String rawInputParams = ((String) inputParams.getValue(execution)).trim();

        if (rawInputParams.isEmpty() || rawInputParams.equals("[]")) {
            log.info("inputParams is empty — no request body.");
            return null;
        }

        // ── MODE 1: Raw JSON object pasted directly (no array wrapper) ────────
        if (rawInputParams.startsWith("{")) {
            log.info("inputParams is a raw JSON object — resolving placeholders and using as body directly.");
            return resolvePlaceholders(rawInputParams, execution, props);
        }

        // ── MODE 2: JSON Array of typed entries ───────────────────────────────
        JSONArray params = new JSONArray(rawInputParams);

        if (params.length() == 0) {
            return null;
        }

        // Will hold keyed entries; used when we're assembling a JSON object body.
        JSONObject bodyObject = new JSONObject();
        // Will be set if a raw body entry (no key) is found.
        String rawBody = null;

        for (int i = 0; i < params.length(); i++) {
            JSONObject entry = params.getJSONObject(i);

            String type  = entry.optString("type", "value");
            String key   = entry.optString("key", "").trim();
            String value = entry.optString("value", "").trim();

            // Resolve all ${...} placeholders in the value string first.
            String resolvedValue = resolvePlaceholders(value, execution, props);

            if ("value".equalsIgnoreCase(type)) {

                if (key.isEmpty()) {
                    // ── NO KEY: treat the entire resolved value as the raw request body ──
                    log.info("inputParams[{}]: type=value, no key → using as raw body.", i);
                    rawBody = resolvedValue;
                    // A raw body entry takes full ownership of the body; break here.
                    break;
                } else {
                    // ── HAS KEY: inject into the body object ──────────────────────────
                    log.info("inputParams[{}]: type=value, key='{}' → resolving value.", i, key);
                    // Try to parse as JSON so it inlines cleanly (not double-serialised).
                    String trimmed = resolvedValue.trim();
                    if (trimmed.startsWith("{")) {
                        try {
                            bodyObject.put(key, new JSONObject(trimmed));
                        } catch (Exception ex) {
                            bodyObject.put(key, resolvedValue);
                        }
                    } else if (trimmed.startsWith("[")) {
                        try {
                            bodyObject.put(key, new JSONArray(trimmed));
                        } catch (Exception ex) {
                            bodyObject.put(key, resolvedValue);
                        }
                    } else {
                        bodyObject.put(key, resolvedValue);
                    }
                }

            } else if ("minioFile".equalsIgnoreCase(type)) {

                if (key.isEmpty()) {
                    throw new BpmnError("CONFIG_ERROR",
                            "inputParams[" + i + "]: type=minioFile requires a 'key' field.");
                }

                log.info("inputParams[{}]: type=minioFile, key='{}', path='{}'", i, key, resolvedValue);

                try {
                    StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);
                    byte[] fileBytes = storage.downloadDocument(resolvedValue).readAllBytes();
                    String base64 = java.util.Base64.getEncoder().encodeToString(fileBytes);
                    bodyObject.put(key, base64);
                    log.info("inputParams[{}]: minioFile '{}' downloaded and base64-encoded → key='{}'",
                            i, resolvedValue, key);
                } catch (Exception e) {
                    log.error("inputParams[{}]: Failed to download minioFile '{}': {}", i, resolvedValue, e.getMessage());
                    throw new BpmnError("STORAGE_ERROR",
                            "inputParams[" + i + "]: Could not download minioFile '" + resolvedValue + "': " + e.getMessage());
                }

            } else if ("minioJson".equalsIgnoreCase(type)) {

                if (key.isEmpty()) {
                    throw new BpmnError("CONFIG_ERROR",
                            "inputParams[" + i + "]: type=minioJson requires a 'key' field.");
                }

                String sourceJsonPath = entry.optString("sourceJsonPath", "").trim();
                log.info("inputParams[{}]: type=minioJson, key='{}', path='{}', sourceJsonPath='{}'",
                        i, key, resolvedValue, sourceJsonPath);

                try {
                    StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);
                    byte[] fileBytes = storage.downloadDocument(resolvedValue).readAllBytes();
                    String fileContent = new String(fileBytes, StandardCharsets.UTF_8);

                    if (sourceJsonPath.isEmpty() || "$".equals(sourceJsonPath)) {
                        // Entire file → inline as JSON object or array
                        String trimmed = fileContent.trim();
                        if (trimmed.startsWith("[")) {
                            bodyObject.put(key, new JSONArray(trimmed));
                        } else {
                            bodyObject.put(key, new JSONObject(trimmed));
                        }
                    } else {
                        // Extract a specific path
                        Configuration jacksonConfig = Configuration.builder()
                                .jsonProvider(new JacksonJsonProvider())
                                .mappingProvider(new JacksonMappingProvider())
                                .build();
                        Object extracted = JsonPath.using(jacksonConfig).parse(fileContent).read(sourceJsonPath);

                        if (extracted instanceof Map) {
                            bodyObject.put(key, new JSONObject((Map<?, ?>) extracted));
                        } else if (extracted instanceof List) {
                            bodyObject.put(key, new JSONArray((List<?>) extracted));
                        } else {
                            bodyObject.put(key, extracted);
                        }
                    }

                    log.info("inputParams[{}]: minioJson '{}' processed → key='{}'", i, resolvedValue, key);

                } catch (Exception e) {
                    log.error("inputParams[{}]: Failed to process minioJson '{}': {}", i, resolvedValue, e.getMessage());
                    throw new BpmnError("STORAGE_ERROR",
                            "inputParams[" + i + "]: Could not process minioJson '" + resolvedValue + "': " + e.getMessage());
                }

            } else {
                log.warn("inputParams[{}]: Unknown type '{}' — skipping entry.", i, type);
            }
        }

        // Decide what to return
        if (rawBody != null) {
            // A no-key value entry provided the full raw body
            return rawBody;
        }

        if (bodyObject.length() > 0) {
            return bodyObject.toString();
        }

        // Nothing built (e.g. all entries were GET-style with no body)
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // UNIFIED OUTPUT MAPPING
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Unified output mapping processor supporting 4 patterns.
     *
     * The source document (responseJson) is the raw API response body.
     * All JsonPaths are evaluated directly against that response.
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
     *  targetPath     (required)  MinIO destination path. Supports ${var} placeholders.
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
     */
    private void processOutputMapping(String responseBody, String outputMappingStr,
                                      String tenantId, DelegateExecution execution,
                                      Properties props) {

        if (outputMappingStr == null || outputMappingStr.trim().equals("{}") || outputMappingStr.trim().isEmpty()) {
            log.info("outputMapping is empty — skipping.");
            return;
        }

        Configuration jacksonConfig = Configuration.builder()
                .jsonProvider(new JacksonJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .build();

        JSONObject mappingConfig = new JSONObject(outputMappingStr);

        for (String outputKey : mappingConfig.keySet()) {
            try {
                JSONObject descriptor = mappingConfig.getJSONObject(outputKey);

                String  storeIn         = descriptor.optString("storeIn", "processVariable");
                boolean mergeVariables  = descriptor.optBoolean("mergeVariables", false);
                String  dataType        = descriptor.optString("dataType", "json");
                String  path            = descriptor.optString("path", "$");

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
                            log.warn("outputMapping key '{}' mergePaths[{}]: missing keyName — skipping.", outputKey, i);
                            continue;
                        }

                        Object val;

                        try {
                            if ("processVariable".equalsIgnoreCase(sourceType)) {
                                // ── SOURCE: Camunda process variable ─────────────────
                                String variableName = mp.optString("variableName", "");
                                if (variableName.isEmpty()) {
                                    log.warn("Merge [{}] entry {}: sourceType=processVariable but variableName is empty — skipping.", outputKey, i);
                                    continue;
                                }
                                Object rawVal = execution.getVariable(variableName);
                                if (rawVal == null) {
                                    log.warn("Merge [{}] entry {}: processVariable '{}' is null — skipping.", outputKey, i, variableName);
                                    continue;
                                }
                                if (rawVal instanceof String) {
                                    String strVal = ((String) rawVal).trim();
                                    if (strVal.startsWith("{")) {
                                        try { val = new JSONObject(strVal); } catch (Exception ig) { val = strVal; }
                                    } else if (strVal.startsWith("[")) {
                                        try { val = new JSONArray(strVal); } catch (Exception ig) { val = strVal; }
                                    } else {
                                        val = strVal;
                                    }
                                } else {
                                    val = rawVal;
                                }
                                log.debug("Merge [{}]: keyName='{}' sourceType=processVariable variableName='{}' type={}",
                                        outputKey, keyName, variableName, val.getClass().getSimpleName());

                            } else if ("minioFile".equalsIgnoreCase(sourceType)) {
                                // ── SOURCE: Existing MinIO file ──────────────────────
                                String resolvedMinioPath = resolveMinioSourcePath(mp, execution, props, outputKey, i);
                                if (resolvedMinioPath == null) continue;

                                String fileContent = downloadMinioFileAsString(resolvedMinioPath, tenantId, outputKey, i);
                                if (fileContent == null) continue;

                                val = extractFromJson(fileContent, mergePath, mergeType, jacksonConfig, outputKey, keyName);
                                if (val == null) continue;

                                log.debug("Merge [{}]: keyName='{}' sourceType=minioFile path='{}' from '{}'",
                                        outputKey, keyName, mergePath, resolvedMinioPath);

                            } else {
                                // ── SOURCE: responseJson (DEFAULT) ───────────────────
                                val = JsonPath.using(jacksonConfig).parse(responseBody).read(mergePath);
                                val = coerceType(val, mergeType);
                                log.debug("Merge [{}]: keyName='{}' sourceType=responseJson path='{}' → '{}'",
                                        outputKey, keyName, mergePath, val);
                            }

                        } catch (Exception e) {
                            log.warn("Merge [{}] entry {}: error resolving keyName='{}' — skipping. Reason: {}",
                                    outputKey, i, keyName, e.getMessage());
                            continue;
                        }

                        // Add to merged object. Duplicate keyNames collect into a JSONArray.
                        putJsonValue(merged, keyName, val);
                    }

                    String serialized = merged.toString();

                    if ("objectStorage".equalsIgnoreCase(storeIn)) {
                        // ── PATTERN D ──────────────────────────────────────────────────
                        String targetPath    = descriptor.optString("targetPath", "");
                        String targetVarName = descriptor.optString("targetVarName", outputKey + "_minioPath");

                        if (targetPath.isEmpty()) {
                            log.warn("outputMapping key '{}': storeIn=objectStorage but targetPath is empty — skipping.", outputKey);
                            continue;
                        }

                        String resolvedTargetPath = resolvePlaceholders(targetPath, execution, props);
                        StorageProvider storage   = ObjectStorageService.getStorageProvider(tenantId);
                        storage.uploadDocument(resolvedTargetPath, serialized.getBytes(StandardCharsets.UTF_8), "application/json");
                        execution.setVariable(targetVarName, resolvedTargetPath);
                        log.info("Output mapping (objectStorage merge): merged → MinIO='{}' → var='{}'",
                                resolvedTargetPath, targetVarName);

                    } else {
                        // ── PATTERN C ──────────────────────────────────────────────────
                        try {
                            ObjectValue objVal = Variables.objectValue(serialized)
                                    .serializationDataFormat("application/json").create();
                            execution.setVariable(outputKey, objVal);
                        } catch (Exception e) {
                            execution.setVariable(outputKey, serialized);
                        }
                        log.info("Output mapping (processVariable merge): set '{}' = merged object", outputKey);
                    }

                } else {
                    // ── PATTERNS A / B: single path extraction ────────────────────────
                    Object val;
                    try {
                        val = JsonPath.using(jacksonConfig).parse(responseBody).read(path);
                        val = coerceType(val, dataType);
                    } catch (Exception e) {
                        log.error("outputMapping key '{}': JsonPath '{}' failed on response — {}", outputKey, path, e.getMessage());
                        throw new BpmnError("OUTPUT_MAPPING_ERROR",
                                "Failed to read path '" + path + "' for key '" + outputKey + "': " + e.getMessage());
                    }

                    if ("objectStorage".equalsIgnoreCase(storeIn)) {
                        // ── PATTERN B ──────────────────────────────────────────────────
                        String targetPath    = descriptor.optString("targetPath", "");
                        String targetVarName = descriptor.optString("targetVarName", outputKey + "_minioPath");

                        if (targetPath.isEmpty()) {
                            log.warn("outputMapping key '{}': storeIn=objectStorage but targetPath is empty — skipping.", outputKey);
                            continue;
                        }

                        String serialized;
                        if (val instanceof JSONObject) {
                            serialized = ((JSONObject) val).toString();
                        } else if (val instanceof JSONArray) {
                            serialized = ((JSONArray) val).toString();
                        } else if (val instanceof Map) {
                            serialized = new JSONObject((Map<?, ?>) val).toString();
                        } else if (val instanceof List) {
                            serialized = new JSONArray((List<?>) val).toString();
                        } else {
                            serialized = val != null ? val.toString() : "null";
                        }

                        String resolvedTargetPath = resolvePlaceholders(targetPath, execution, props);
                        StorageProvider storage   = ObjectStorageService.getStorageProvider(tenantId);
                        storage.uploadDocument(resolvedTargetPath, serialized.getBytes(StandardCharsets.UTF_8), "application/json");
                        execution.setVariable(targetVarName, resolvedTargetPath);
                        log.info("Output mapping (objectStorage): path='{}' → MinIO='{}' → var='{}'",
                                path, resolvedTargetPath, targetVarName);

                    } else {
                        // ── PATTERN A ──────────────────────────────────────────────────
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
    // MINIO HELPERS
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Resolves the MinIO source path for a mergePath entry with sourceType=minioFile.
     * Returns null (and logs a warning) if resolution fails.
     */
    private String resolveMinioSourcePath(JSONObject mp, DelegateExecution execution,
                                          Properties props, String outputKey, int entryIndex) {
        String sourcePath   = mp.optString("sourcePath", "");
        String variableName = mp.optString("variableName", "");

        if (!sourcePath.isEmpty()) {
            return resolvePlaceholders(sourcePath, execution, props);
        } else if (!variableName.isEmpty()) {
            Object varVal = execution.getVariable(variableName);
            if (varVal == null) {
                log.warn("Merge [{}] entry {}: sourceType=minioFile variableName='{}' is null — skipping",
                        outputKey, entryIndex, variableName);
                return null;
            }
            return varVal.toString();
        } else {
            log.warn("Merge [{}] entry {}: sourceType=minioFile but neither sourcePath nor variableName provided — skipping",
                    outputKey, entryIndex);
            return null;
        }
    }

    /**
     * Downloads a MinIO file and returns its content as a String.
     * Returns null (and logs a warning) if download fails.
     */
    private String downloadMinioFileAsString(String minioPath, String tenantId,
                                             String outputKey, int entryIndex) {
        try {
            StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);
            byte[] bytes = storage.downloadDocument(minioPath).readAllBytes();
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.warn("Merge [{}] entry {}: failed to download minioFile '{}' — skipping. Reason: {}",
                    outputKey, entryIndex, minioPath, e.getMessage());
            return null;
        }
    }

    /**
     * Extracts a value from a JSON string using JsonPath, optionally coercing it to a dataType.
     * Returns null and logs a warning if extraction fails.
     */
    private Object extractFromJson(String jsonContent, String path, String dataType,
                                   Configuration jacksonConfig, String outputKey, String keyName) {
        try {
            Object val = JsonPath.using(jacksonConfig).parse(jsonContent).read(path);
            return coerceType(val, dataType);
        } catch (Exception e) {
            log.warn("Merge [{}]: keyName='{}' JsonPath '{}' failed — skipping. Reason: {}",
                    outputKey, keyName, path, e.getMessage());
            return null;
        }
    }

    /**
     * Coerces a value to the requested dataType.
     *   "string"  -> calls toString()
     *   "long"    -> converts Number to Long (safely handles 70.0 doubles from scoring APIs)
     *   "int"     -> converts Number to Integer
     *   "json"    -> returns as-is (default)
     */
    private Object coerceType(Object val, String dataType) {
        if (val == null) return null;
        if ("string".equalsIgnoreCase(dataType)) {
            return val instanceof String ? val : val.toString();
        }
        if ("long".equalsIgnoreCase(dataType)) {
            if (val instanceof Long)   return val;
            if (val instanceof Number) return ((Number) val).longValue();
            try { return Long.parseLong(val.toString().trim().replaceAll("[.][0-9]+$", "")); }
            catch (Exception e) { log.warn("coerceType long: could not convert {} to Long, returning as-is", val); }
        }
        if ("int".equalsIgnoreCase(dataType) || "integer".equalsIgnoreCase(dataType)) {
            if (val instanceof Integer) return val;
            if (val instanceof Number)  return ((Number) val).intValue();
            try { return Integer.parseInt(val.toString().trim().replaceAll("[.][0-9]+$", "")); }
            catch (Exception e) { log.warn("coerceType int: could not convert {} to Integer, returning as-is", val); }
        }
        return val;
    }

    /**
     * Puts a value into a JSONObject by key.
     * If the key already exists, converts to a JSONArray and appends.
     * Handles all common types safely.
     */
    private void putJsonValue(JSONObject obj, String key, Object val) throws Exception {
        if (obj.has(key)) {
            Object existing = obj.get(key);
            JSONArray arr;
            if (existing instanceof JSONArray) {
                arr = (JSONArray) existing;
            } else {
                arr = new JSONArray();
                addToArray(arr, existing);
            }
            addToArray(arr, val);
            obj.put(key, arr);
        } else {
            if (val == null)                   obj.put(key, JSONObject.NULL);
            else if (val instanceof JSONObject) obj.put(key, (JSONObject) val);
            else if (val instanceof JSONArray)  obj.put(key, (JSONArray) val);
            else if (val instanceof Map)        obj.put(key, new JSONObject((Map<?, ?>) val));
            else if (val instanceof List)       obj.put(key, new JSONArray((List<?>) val));
            else if (val instanceof Boolean)    obj.put(key, (boolean) val);
            else if (val instanceof Integer)    obj.put(key, (int) val);
            else if (val instanceof Long)       obj.put(key, (long) val);
            else if (val instanceof Double)     obj.put(key, (double) val);
            else                                obj.put(key, val.toString());
        }
    }

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

    // ─────────────────────────────────────────────────────────────────────────
    // HTTP HELPERS
    // ─────────────────────────────────────────────────────────────────────────

    private HttpRequestBase createRequest(String method, String url, String body) throws Exception {
        switch (method.toUpperCase()) {
            case "POST":
                HttpPost post = new HttpPost(url);
                if (body != null && !body.isEmpty()) post.setEntity(new StringEntity(body));
                return post;
            case "PUT":
                HttpPut put = new HttpPut(url);
                if (body != null && !body.isEmpty()) put.setEntity(new StringEntity(body));
                return put;
            case "DELETE":
                return new HttpDelete(url);
            case "GET":
            default:
                return new HttpGet(url);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PLACEHOLDER RESOLUTION
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Resolves ${...} placeholders in a string.
     *
     * Supported patterns:
     *   ${fn.uuid}             → random UUID
     *   ${fn.isoTimestamp}     → current UTC ISO-8601 timestamp
     *   ${fn.randomTxnId}      → single uppercase letter + epoch millis
     *   ${appproperties.key}   → value from tenant application.properties
     *   ${processVariable.key} → value from Camunda process variable
     *   ${key}                 → shorthand for processVariable
     */
    private String resolvePlaceholders(String input, DelegateExecution execution, Properties props) {
        if (input == null || input.isEmpty()) return input;

        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(input);
        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            String token = matcher.group(1);
            String replacement = "";

            if (token.startsWith("fn.")) {
                String fnName = token.substring("fn.".length());
                switch (fnName) {
                    case "uuid":
                        replacement = UUID.randomUUID().toString();
                        break;
                    case "isoTimestamp":
                        replacement = Instant.now().toString();
                        break;
                    case "randomTxnId":
                        char letter = (char) ('A' + new Random().nextInt(26));
                        replacement = letter + String.valueOf(System.currentTimeMillis());
                        break;
                    default:
                        log.warn("Unknown fn placeholder: {}", token);
                }

            } else if (token.startsWith("appproperties.")) {
                String propKey = token.substring("appproperties.".length());
                replacement = props.getProperty(propKey, "");

            } else if (token.startsWith("processVariable.")) {
                String varKey = token.substring("processVariable.".length());
                Object varVal = execution.getVariable(varKey);
                replacement = varVal != null ? varVal.toString() : "";

            } else {
                // Shorthand: ${someVar} → process variable
                Object varVal = execution.getVariable(token);
                replacement = varVal != null ? varVal.toString() : "";
            }

            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }

        matcher.appendTail(result);
        return result.toString();
    }
}