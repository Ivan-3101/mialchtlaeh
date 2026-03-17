package com.DronaPay.generic.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.Expression;
import org.cibseven.bpm.engine.delegate.JavaDelegate;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@Slf4j
public class GenericTicketIdGenerator_V2 implements JavaDelegate {

    // Full stage config JSON pasted directly into the Element Template field
    private Expression stageConfig;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        log.info("=== GenericTicketIdGenerator_V2 Started ===");

        // 1. Read stageConfig from injected field
        String stageConfigStr = (stageConfig != null) ? (String) stageConfig.getValue(execution) : null;
        if (stageConfigStr == null || stageConfigStr.trim().isEmpty()) {
            throw new RuntimeException("GenericTicketIdGenerator_V2: 'stageConfig' field is missing or empty in BPMN.");
        }

        JSONObject config = new JSONObject(stageConfigStr);

        // 2. Resolve context
        String tenantId = execution.getTenantId();
        String workflowKey = execution.getProcessEngineServices().getRepositoryService()
                .getProcessDefinition(execution.getProcessDefinitionId()).getKey();

        log.info("Tenant: {}, WorkflowKey: {}", tenantId, workflowKey);

        // 3. Build rootObj from initialVariablesRootObj
        ObjectNode rootObj = mapper.createObjectNode();
        if (config.has("initialVariablesRootObj")) {
            JSONArray initVars = config.getJSONArray("initialVariablesRootObj");
            for (int i = 0; i < initVars.length(); i++) {
                JSONObject varConf = initVars.getJSONObject(i);
                String key = varConf.getString("key");

                if (varConf.optBoolean("executionMethod", false)) {
                    switch (key) {
                        case "tenantId":
                            rootObj.put(key, tenantId);
                            execution.setVariable("tenantId", tenantId);
                            break;
                        case "workflowKey":
                            rootObj.put(key, workflowKey);
                            execution.setVariable("workflowKey", workflowKey);
                            break;
                        case "processInstanceId":
                            rootObj.put(key, execution.getProcessInstanceId());
                            execution.setVariable("processInstanceId", execution.getProcessInstanceId());
                            break;
                        case "stageName":
                            rootObj.put(key, execution.getCurrentActivityId());
                            execution.setVariable("stageName", execution.getCurrentActivityId());
                            break;
                        default:
                            log.warn("Unknown executionMethod variable: {}", key);
                    }
                } else if (varConf.optBoolean("processVariable", false)) {
                    String source = varConf.optString("source", key);
                    Object val = execution.getVariable(source);
                    if (val != null) {
                        rootObj.put(key, val.toString());
                    } else {
                        log.warn("Process variable '{}' is null — skipping from rootObj", source);
                    }
                }
            }
        }

        log.info("rootObj built: {}", rootObj);

        // 4. Execute steps
        Connection connection = null;
        try {
            connection = execution.getProcessEngine()
                    .getProcessEngineConfiguration()
                    .getDataSource()
                    .getConnection();

            if (config.has("steps")) {
                JSONArray steps = config.getJSONArray("steps");
                for (int i = 0; i < steps.length(); i++) {
                    JSONObject step = steps.getJSONObject(i);
                    String stepKey = step.optString("key", "step_" + i);
                    String stepType = step.optString("type", "");
                    log.info("Executing step: {} (type: {})", stepKey, stepType);

                    switch (stepType) {
                        case "SqlQueryExecution":
                            executeSqlStep(step, rootObj, execution, connection);
                            break;
                        case "SetProcessVariables":
                            executeSetVariablesStep(step, rootObj, execution);
                            break;
                        default:
                            log.warn("Unknown step type '{}' for step '{}' — skipping", stepType, stepKey);
                    }
                }
            }
        } finally {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }

        log.info("=== GenericTicketIdGenerator_V2 Completed ===");
    }

    private void executeSqlStep(JSONObject step, ObjectNode rootObj,
                                DelegateExecution execution, Connection connection) throws Exception {
        JSONObject sqlConfig = step.getJSONObject("config");
        String query = sqlConfig.getString("query");
        String queryType = sqlConfig.getString("queryType");
        JSONArray parameters = sqlConfig.optJSONArray("parameters");

        // Build parameter values — skip entire step if any required param is null
        Object[] paramValues = new Object[parameters != null ? parameters.length() : 0];
        if (parameters != null) {
            for (int i = 0; i < parameters.length(); i++) {
                String paramKey = parameters.getJSONObject(i).getString("key");
                String value = rootObj.has(paramKey) ? rootObj.get(paramKey).asText(null) : null;
                if (value == null) {
                    log.warn("Step '{}': parameter '{}' is null — skipping step",
                            step.optString("key"), paramKey);
                    return;
                }
                paramValues[i] = value;
            }
        }

        PreparedStatement stmt = connection.prepareStatement(query);
        for (int i = 0; i < paramValues.length; i++) {
            stmt.setString(i + 1, paramValues[i].toString());
        }

        if ("update".equalsIgnoreCase(queryType)) {
            int rows = stmt.executeUpdate();
            log.info("Step '{}': update executed, rows affected: {}", step.optString("key"), rows);

        } else if ("select".equalsIgnoreCase(queryType)) {
            ResultSet rs = stmt.executeQuery();
            String returnValueKey = step.optString("returnValueKey", "");
            String returnValueObjKey = step.optString("returnValueObjKey", "");

            if (rs.next() && !returnValueKey.isEmpty()) {
                String value = rs.getString(returnValueKey);
                log.info("Step '{}': query returned '{}' = {}", step.optString("key"), returnValueKey, value);

                // Store in rootObj
                rootObj.put(returnValueObjKey.isEmpty() ? returnValueKey : returnValueObjKey, value);

                // Set as process variables
                JSONArray varsToSet = step.optJSONArray("processVariablesToSetAfterExecution");
                if (varsToSet != null) {
                    for (int i = 0; i < varsToSet.length(); i++) {
                        JSONObject varConf = varsToSet.getJSONObject(i);
                        String varKey = varConf.getString("key");
                        String path = varConf.getString("path");
                        String resolvedValue = rootObj.has(path) ? rootObj.get(path).asText() : value;
                        execution.setVariable(varKey, resolvedValue);
                        log.info("Set process variable '{}' = {}", varKey, resolvedValue);
                    }
                }
            } else {
                log.warn("Step '{}': select returned no rows", step.optString("key"));
            }
            rs.close();
        }

        stmt.close();
    }

    private void executeSetVariablesStep(JSONObject step, ObjectNode rootObj,
                                         DelegateExecution execution) {
        JSONArray variables = step.optJSONArray("variables");
        if (variables == null) return;

        for (int i = 0; i < variables.length(); i++) {
            JSONObject varConf = variables.getJSONObject(i);
            String key = varConf.getString("key");

            if (varConf.has("staticValue")) {
                String val = varConf.get("staticValue").toString();
                execution.setVariable(key, val);
                rootObj.put(key, val);
                log.info("Set static variable '{}' = {}", key, val);
            } else if (varConf.has("sourcePath")) {
                String sourcePath = varConf.getString("sourcePath");
                if (rootObj.has(sourcePath)) {
                    String val = rootObj.get(sourcePath).asText();
                    execution.setVariable(key, val);
                    log.info("Set variable '{}' from rootObj path '{}' = {}", key, sourcePath, val);
                } else {
                    log.warn("SetProcessVariables: sourcePath '{}' not found in rootObj", sourcePath);
                }
            }
        }
    }
}