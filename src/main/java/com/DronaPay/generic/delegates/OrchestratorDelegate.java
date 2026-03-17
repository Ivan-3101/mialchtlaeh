package com.DronaPay.generic.delegates;

import com.DronaPay.generic.services.ObjectStorageService;
import com.DronaPay.generic.storage.StorageProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.JavaDelegate;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * OrchestratorDelegate — Central state machine for the Rule Creation agent loop.
 *
 * Reads 7 stage configurations from camunda:inputParameter fields injected by the
 * Orchestrator Element Template. Skips any stage where stageNType == "none".
 * Manages context.json in MinIO as the single source of truth for session state.
 *
 * Entry conditions:
 *   - First run    : currentStepIndex is null (not yet set as process variable)
 *   - From Agent   : currentStepIndex is set, userAnswers is null
 *   - From UserTask: currentStepIndex is set, userAnswers is non-null
 *
 * Sets orchestratorAction to one of: CONTINUE | RETRY_STEP | GO_USER_TASK | EXIT
 * GW1 routes on this variable.
 *
 * Process variables set for the Generic Agent Task (read by OrchestratorAgentDelegate):
 *   - currentAgentId       : String — agent ID for this stage
 *   - currentStageType     : String — stage type slug (e.g. "execution-mode")
 *   - currentInputParams   : String — JSON array describing the DIA request data payload
 *   - currentOutputMapping : String — JSON array for output routing after DIA response
 *   - currentStepIndex     : Integer — zero-based index of current stage
 *
 * stepsConfig is stored as a List<Map<String, Object>> (Jackson/Camunda Object variable).
 * This avoids Camunda's 4000-character limit on String variables while remaining
 * fully readable in Camunda Cockpit as a serialised Java object.
 */
@Slf4j
public class OrchestratorDelegate implements JavaDelegate {

    private static final String WORKFLOW_KEY    = "RuleCreation";
    private static final String CONTEXT_FILE    = "context.json";
    private static final int    MAX_STAGE_SLOTS = 7;

    @Override
    public void execute(DelegateExecution execution) throws Exception {

        String tenantId  = execution.getTenantId();
        execution.setVariable("tenantId", tenantId);
        String ticketId  = String.valueOf(execution.getVariable("TicketID"));
        String userInput = (String) execution.getVariable("userInput");

        log.info("=== OrchestratorDelegate | TicketID: {} | Tenant: {} ===", ticketId, tenantId);

        StorageProvider storage     = ObjectStorageService.getStorageProvider(tenantId);
        String          contextPath = buildContextPath(tenantId, ticketId);

        // contextPath is passed directly into setCurrentStepVars() where it resolves
        // the ##contextMinioPath## placeholder in currentInputParams at the time the
        // step variable is set. No separate process variable needed.

        Integer currentStepIndex = toInteger(execution.getVariable("currentStepIndex"));

        // ═══════════════════════════════════════════════════════════════════════
        // FIRST RUN — initialise everything
        // ═══════════════════════════════════════════════════════════════════════
        if (currentStepIndex == null) {
            log.info("First run — initialising orchestrator state");

            List<JSONObject> steps = buildSteps(execution);
            if (steps.isEmpty()) {
                throw new RuntimeException(
                        "OrchestratorDelegate: No stages configured — all stageNType values are 'none'.");
            }

            int totalSteps = steps.size();

            JSONObject context = new JSONObject();
            context.put("ticketId",            ticketId);
            context.put("workflowKey",         WORKFLOW_KEY);
            context.put("tenantId",            tenantId);
            context.put("userInput",           userInput != null ? userInput : "");
            context.put("currentStepIndex",    0);
            context.put("totalSteps",          totalSteps);
            context.put("workflowStatus",      "RUNNING");
            context.put("extracted_values",    new JSONObject());
            context.put("interaction_history", new JSONArray());

            uploadContext(storage, contextPath, context);

            // ── Option C: store stepsConfig as List<Map> (no 4000-char limit) ──
            List<Map<String, Object>> stepMaps = new ArrayList<>();
            for (JSONObject step : steps) {
                stepMaps.add(step.toMap());
            }
            execution.setVariable("stepsConfig",      stepMaps);
            // ────────────────────────────────────────────────────────────────────

            execution.setVariable("currentStepIndex", 0);
            execution.setVariable("retryCount",       0);
            execution.setVariable("totalSteps",       totalSteps);
            execution.setVariable("workflowStatus",   "RUNNING");

            setCurrentStepVars(execution, steps.get(0), 0, contextPath);
            execution.setVariable("orchestratorAction", "CONTINUE");

            log.info("Init complete. totalSteps={} | Stage 0 | agentId={} | action=CONTINUE",
                    totalSteps, steps.get(0).optString("agentId"));
            return;
        }

        // ═══════════════════════════════════════════════════════════════════════
        // SUBSEQUENT RUNS — load state
        // ═══════════════════════════════════════════════════════════════════════
        List<JSONObject> steps     = loadSteps(execution);
        int              totalSteps = toIntVar(execution, "totalSteps", steps.size());
        int              retryCount = toIntVar(execution, "retryCount", 0);
        JSONObject       context    = downloadContext(storage, contextPath);

        // ── RETURN FROM USER TASK ────────────────────────────────────────────
        String userAnswers = getStringVar(execution, "userAnswers");
        if (userAnswers != null && !userAnswers.trim().isEmpty()) {
            log.info("Returning from User Task | stage={}", currentStepIndex);

            // Parse user answers
            JSONObject answersObj;
            try {
                answersObj = new JSONObject(userAnswers);
            } catch (Exception e) {
                log.warn("userAnswers is not valid JSON — wrapping as raw string");
                answersObj = new JSONObject();
                answersObj.put("raw", userAnswers);
            }

            // Append USER_TASK entry to interaction history
            JSONArray history = context.optJSONArray("interaction_history");
            if (history == null) history = new JSONArray();

            JSONObject userEntry = new JSONObject();
            userEntry.put("stage",          currentStepIndex);
            userEntry.put("agentId",        getStringVar(execution, "currentAgentId"));
            userEntry.put("from",           "USER_TASK");
            userEntry.put("userAnswers",    answersObj);
            userEntry.put("questions",      toJsonArray(execution.getVariable("agentQuestions")));
            userEntry.put("missing_fields", toJsonArray(execution.getVariable("agentMissingFields")));
            history.put(userEntry);
            context.put("interaction_history", history);

            // Clear userAnswers so next Orchestrator run doesn't re-enter this branch
            execution.setVariable("userAnswers", null);

            uploadContext(storage, contextPath, context);

            execution.setVariable("orchestratorAction", "RETRY_STEP");
            execution.setVariable("workflowStatus",     "RUNNING");
            log.info("User Task return processed | action=RETRY_STEP | stage={}", currentStepIndex);

            log.info("=== OrchestratorDelegate Complete | action=RETRY_STEP ===");
            return;
        }

        // ── RETURN FROM AGENT ────────────────────────────────────────────────
        String lastAgentResultStr = getStringVar(execution, "lastAgentResult");
        JSONObject lastAgentResult;
        try {
            lastAgentResult = new JSONObject(
                    lastAgentResultStr != null ? lastAgentResultStr : "{}");
        } catch (Exception e) {
            log.warn("lastAgentResult is not valid JSON — treating as empty");
            lastAgentResult = new JSONObject();
        }

        String agentAction = lastAgentResult.optString("action", "FAIL");
        log.info("Returning from Agent | agentAction={} | stage={}", agentAction, currentStepIndex);

        // Merge updated_context into extracted_values regardless of action
        mergeUpdatedContext(context, lastAgentResult);

        // Append AGENT entry to interaction history
        JSONArray history = context.optJSONArray("interaction_history");
        if (history == null) history = new JSONArray();

        JSONObject agentEntry = new JSONObject();
        agentEntry.put("stage",      currentStepIndex);
        agentEntry.put("agentId",    getStringVar(execution, "currentAgentId"));
        agentEntry.put("from",       "AGENT");
        agentEntry.put("action",     agentAction);
        agentEntry.put("status",     lastAgentResult.optString("status", ""));
        agentEntry.put("confidence", lastAgentResult.opt("confidence"));
        history.put(agentEntry);
        context.put("interaction_history", history);

        JSONObject currentStep = steps.get(currentStepIndex);
        String     onFailure   = currentStep.optString("onFailure",  "END");
        int        maxRetries  = currentStep.optInt("maxRetries", 0);

        switch (agentAction) {

            // ── NEXT_STEP ────────────────────────────────────────────────────
            case "NEXT_STEP": {
                int nextIndex = currentStepIndex + 1;
                context.put("currentStepIndex", nextIndex);
                execution.setVariable("retryCount", 0);

                if (nextIndex >= totalSteps) {
                    context.put("workflowStatus", "DONE");
                    uploadContext(storage, contextPath, context);
                    execution.setVariable("currentStepIndex", nextIndex);
                    execution.setVariable("orchestratorAction", "EXIT");
                    execution.setVariable("workflowStatus",     "DONE");
                } else {
                    context.put("workflowStatus", "RUNNING");
                    uploadContext(storage, contextPath, context);
                    execution.setVariable("currentStepIndex", nextIndex);
                    setCurrentStepVars(execution, steps.get(nextIndex), nextIndex, contextPath);
                    execution.setVariable("orchestratorAction", "CONTINUE");
                    execution.setVariable("workflowStatus",     "RUNNING");
                }
                break;
            }

            // ── GO_USER_TASK ─────────────────────────────────────────────────
            case "GO_USER_TASK": {
                uploadContext(storage, contextPath, context);
                execution.setVariable("orchestratorAction", "GO_USER_TASK");
                execution.setVariable("workflowStatus",     "AWAITING_USER");
                log.info("Agent requested user input | action=GO_USER_TASK | stage={}", currentStepIndex);
                break;
            }

            // ── RETRY_STEP (agent-requested) ──────────────────────────────────
            case "RETRY_STEP": {
                int newRetryCount = retryCount + 1;
                execution.setVariable("retryCount", newRetryCount);
                uploadContext(storage, contextPath, context);

                if (newRetryCount > maxRetries) {
                    log.warn("Max retries ({}) exceeded at stage {} | on_failure={}",
                            maxRetries, currentStepIndex, onFailure);
                    applyOnFailure(execution, context, storage, contextPath,
                            onFailure, currentStepIndex, steps);
                } else {
                    execution.setVariable("orchestratorAction", "RETRY_STEP");
                    execution.setVariable("workflowStatus",     "RUNNING");
                    log.info("Agent retry | retryCount={}/{} | action=RETRY_STEP | stage={}",
                            newRetryCount, maxRetries, currentStepIndex);
                }
                break;
            }

            // ── END ──────────────────────────────────────────────────────────
            case "END": {
                uploadContext(storage, contextPath, context);
                execution.setVariable("orchestratorAction", "EXIT");
                execution.setVariable("workflowStatus",     "DONE");
                log.info("Agent signalled END | action=EXIT");
                break;
            }

            // ── FAIL ─────────────────────────────────────────────────────────
            case "FAIL": {
                log.warn("Agent signalled FAIL at stage {} | on_failure={}", currentStepIndex, onFailure);
                uploadContext(storage, contextPath, context);
                applyOnFailure(execution, context, storage, contextPath,
                        onFailure, currentStepIndex, steps);
                break;
            }

            default: {
                log.error("Unknown agentAction '{}' at stage {} — forcing EXIT", agentAction, currentStepIndex);
                uploadContext(storage, contextPath, context);
                execution.setVariable("orchestratorAction", "EXIT");
                execution.setVariable("workflowStatus",     "ERROR");
                break;
            }
        }

        log.info("=== OrchestratorDelegate Complete | action={} ===",
                execution.getVariable("orchestratorAction"));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // PRIVATE HELPERS
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Builds the steps list from camunda:inputParameter values injected by the
     * Orchestrator Element Template. Called only on first run.
     *
     * Reads the following fields per stage slot N (1–7):
     *   stageNType, stageNAgentId, stageNAllowUserInput, stageNMaxRetries,
     *   stageNOnFailure, stageNInputParams, stageNOutputMapping
     *
     * Skips any slot where stageNType is null, "none", or "NA".
     */
    private List<JSONObject> buildSteps(DelegateExecution execution) {
        List<JSONObject> steps = new ArrayList<>();
        for (int i = 1; i <= MAX_STAGE_SLOTS; i++) {
            String type = getStringVar(execution, "stage" + i + "Type");
            if (type == null || type.equalsIgnoreCase("none") || type.equalsIgnoreCase("NA")) continue;

            JSONObject step = new JSONObject();
            step.put("stageType",      type);
            step.put("agentId",        getStringVar(execution, "stage" + i + "AgentId"));
            step.put("allowUserInput", getStringVar(execution, "stage" + i + "AllowUserInput"));
            step.put("maxRetries",     parseIntSafe(getStringVar(execution, "stage" + i + "MaxRetries")));
            step.put("onFailure",      getStringVar(execution, "stage" + i + "OnFailure"));
            step.put("inputParams",    getStringVar(execution, "stage" + i + "InputParams"));
            step.put("outputMapping",  getStringVar(execution, "stage" + i + "OutputMapping"));
            steps.add(step);

            log.debug("Stage slot {} loaded: type={} agentId={}", i, type, step.optString("agentId"));
        }
        log.info("Built {} active stage(s) from template config", steps.size());
        return steps;
    }

    /**
     * Loads steps from the stepsConfig process variable (persisted on first run).
     * Used on all subsequent runs to avoid relying on camunda:inputParameter scope.
     *
     * stepsConfig is stored as a List<Map<String, Object>> (Option C — Jackson serialisation).
     * Each Map entry is reconstructed into a JSONObject for use by the rest of the delegate.
     * Falls back to rebuilding from Element Template inputs if the variable is missing or
     * of an unexpected type.
     */
    private List<JSONObject> loadSteps(DelegateExecution execution) {
        List<JSONObject> steps = new ArrayList<>();
        Object raw = execution.getVariable("stepsConfig");

        if (raw == null) {
            log.warn("stepsConfig process variable not found — rebuilding from input params");
            return buildSteps(execution);
        }

        if (raw instanceof List) {
            List<?> list = (List<?>) raw;
            for (Object item : list) {
                if (item instanceof Map) {
                    steps.add(new JSONObject((Map<?, ?>) item));
                }
            }
            return steps;
        }

        // Fallback: handle byte[] or String if somehow stored that way
        String stepsConfigStr = null;
        if (raw instanceof byte[]) {
            stepsConfigStr = new String((byte[]) raw, StandardCharsets.UTF_8);
        } else if (raw instanceof String) {
            stepsConfigStr = (String) raw;
        }

        if (stepsConfigStr != null && !stepsConfigStr.isEmpty()) {
            JSONArray arr = new JSONArray(stepsConfigStr);
            for (int i = 0; i < arr.length(); i++) {
                steps.add(arr.getJSONObject(i));
            }
            return steps;
        }

        log.warn("stepsConfig is unexpected type: {} — rebuilding from input params",
                raw.getClass().getName());
        return buildSteps(execution);
    }

    /**
     * Sets the process variables that OrchestratorAgentDelegate reads via its
     * Expression fields and placeholder resolution:
     *
     *   currentAgentId       — resolves ${currentAgentId} in the agentId field
     *   currentStageType     — used to derive the MinIO stage folder name
     *   currentInputParams   — resolves ${currentInputParams} in the inputParams field
     *   currentOutputMapping — resolves ${currentOutputMapping} in the outputMapping field
     *   currentStepIndex     — current zero-based position in the steps array
     *
     * The stageNInputParams value stored in the template uses ##contextMinioPath## as a
     * custom non-JUEL placeholder (using ${...} would cause the Camunda engine to evaluate
     * it as a JUEL expression at task-entry time, before OrchestratorDelegate runs).
     * This method substitutes ##contextMinioPath## with the actual MinIO path before
     * storing currentInputParams as a process variable.
     */
    private void setCurrentStepVars(DelegateExecution execution, JSONObject step,
                                    int index, String contextPath) {
        String rawInputParams      = step.optString("inputParams",    "[]");
        String resolvedInputParams = rawInputParams.replace("##contextMinioPath##", contextPath);

        execution.setVariable("currentAgentId",        step.optString("agentId",       "unknown-agent"));
        execution.setVariable("currentStageType",      step.optString("stageType",     "unknown"));
        execution.setVariable("currentInputParams",    resolvedInputParams);
        execution.setVariable("currentOutputMapping",  step.optString("outputMapping", "[]"));
        execution.setVariable("currentStepIndex",      index);
    }

    /**
     * Applies the on_failure strategy when max retries are exceeded or agent signals FAIL.
     * RETRY → keeps looping (RETRY_STEP)
     * END   → exits workflow (EXIT)
     */
    private void applyOnFailure(DelegateExecution execution, JSONObject context,
                                StorageProvider storage, String contextPath,
                                String onFailure, int currentStepIndex,
                                List<JSONObject> steps) throws Exception {
        if ("RETRY".equalsIgnoreCase(onFailure)) {
            execution.setVariable("orchestratorAction", "RETRY_STEP");
            execution.setVariable("workflowStatus",     "RUNNING");
            log.info("on_failure=RETRY — RETRY_STEP at stage {}", currentStepIndex);
        } else {
            execution.setVariable("orchestratorAction", "EXIT");
            execution.setVariable("workflowStatus",     "DONE");
            log.info("on_failure=END — EXIT at stage {}", currentStepIndex);
        }
    }

    /**
     * Merges the agent's updated_context into context.json extracted_values.
     * Called on every agent return regardless of agentAction.
     */
    private void mergeUpdatedContext(JSONObject context, JSONObject agentResult) {
        try {
            JSONObject updatedContext = agentResult.optJSONObject("updated_context");
            if (updatedContext == null || updatedContext.isEmpty()) return;

            JSONObject extracted = context.optJSONObject("extracted_values");
            if (extracted == null) extracted = new JSONObject();

            for (String key : updatedContext.keySet()) {
                extracted.put(key, updatedContext.get(key));
            }
            context.put("extracted_values", extracted);
            log.debug("Merged {} updated_context key(s): {}", updatedContext.length(), updatedContext.keySet());
        } catch (Exception e) {
            log.warn("Failed to merge updated_context — skipping. Reason: {}", e.getMessage());
        }
    }

    private JSONObject downloadContext(StorageProvider storage, String path) throws Exception {
        try (InputStream is = storage.downloadDocument(path)) {
            String json = IOUtils.toString(is, StandardCharsets.UTF_8);
            return new JSONObject(json);
        }
    }

    private void uploadContext(StorageProvider storage, String path, JSONObject context) throws Exception {
        byte[] bytes = context.toString(2).getBytes(StandardCharsets.UTF_8);
        storage.uploadDocument(path, bytes, "application/json");
        log.debug("context.json uploaded to {}", path);
    }

    private String buildContextPath(String tenantId, String ticketId) {
        return tenantId + "/" + WORKFLOW_KEY + "/" + ticketId + "/" + CONTEXT_FILE;
    }

    private JSONArray stepsToJsonArray(List<JSONObject> steps) {
        JSONArray arr = new JSONArray();
        steps.forEach(arr::put);
        return arr;
    }

    private String getStringVar(DelegateExecution execution, String name) {
        Object val = execution.getVariable(name);
        return val != null ? val.toString() : null;
    }

    private Integer toInteger(Object val) {
        if (val == null) return null;
        try { return Integer.parseInt(val.toString()); }
        catch (NumberFormatException e) { return null; }
    }

    private int toIntVar(DelegateExecution execution, String name, int defaultValue) {
        Object val = execution.getVariable(name);
        if (val == null) return defaultValue;
        try { return Integer.parseInt(val.toString()); }
        catch (NumberFormatException e) { return defaultValue; }
    }

    private int parseIntSafe(String value) {
        if (value == null || value.equalsIgnoreCase("NA") || value.trim().isEmpty()) return 0;
        try { return Integer.parseInt(value.trim()); }
        catch (NumberFormatException e) { return 0; }
    }

    private JSONArray toJsonArray(Object val) {
        if (val == null) return new JSONArray();
        try { return new JSONArray(val.toString()); }
        catch (Exception e) { return new JSONArray(); }
    }
}