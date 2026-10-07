/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.tests.system.connectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Provider lifecycle for a NAR-swap upgrade and a version change. This class intentionally does not inherit
 * {@link ConnectorUpgradeDrainIT}'s tests; those run without a provider.
 */
@ExtendWith(ConnectorUpgradeDrainProviderIT.OnlyProviderTests.class)
public class ConnectorUpgradeDrainProviderIT extends ConnectorUpgradeDrainIT {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String LEGACY_NOTE = "Legacy Note";
    private static final String DRAIN_LABEL = "Drain Label";

    private final Path providerState = createProviderState();

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of(
            NiFiProperties.CONNECTOR_CONFIGURATION_PROVIDER_IMPLEMENTATION,
            "org.apache.nifi.connectors.tests.system.RecordingConnectorConfigurationProvider",
            NiFiProperties.CONNECTOR_CONFIGURATION_PROVIDER_PROPERTIES_PREFIX + "state.directory",
            providerState.toAbsolutePath().toString());
    }

    @Test
    @Timeout(240)
    public void testProviderConfigurationMigratedWithoutImplicitSave() throws Exception {
        final Map<String, String> initial = properties("DRAIN", false, true, "shared");
        initial.put(LEGACY_NOTE, "v1-only");
        final String connectorId = createConnector("UpgradeDrainConnector", initial);
        final int journalBeforeRestart = journal().size();

        swapAndRestart();
        assertConnectorState(connectorId, org.apache.nifi.components.connector.ConnectorState.STOPPED);
        assertPending(connectorId);
        final ConnectorEntity pending = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertNull(configurationValue(pending.getComponent().getActiveConfiguration(), LEGACY_NOTE));
        assertNull(configurationValue(pending.getComponent().getWorkingConfiguration(), LEGACY_NOTE));
        assertEquals("upgraded", configurationValue(pending.getComponent().getActiveConfiguration(), DRAIN_LABEL));
        assertEquals("upgraded", configurationValue(pending.getComponent().getWorkingConfiguration(), DRAIN_LABEL));
        final String duringRestart = journalSince(journalBeforeRestart);
        assertTrue(duringRestart.contains("GET_SYNC_DIRECTIVE"), duringRestart);
        assertFalse(duringRestart.contains("SAVE"), duringRestart);
        assertFalse(duringRestart.contains("SHOULD_APPLY_UPDATE"), duringRestart);
        assertFalse(duringRestart.contains("SYNC_ASSETS"), duringRestart);

        final int journalBeforeEdit = journal().size();
        getClientUtil().configureConnector(connectorId, "Upgrade", properties("DRAIN", false, true, "edited"));
        final String duringEdit = journalSince(journalBeforeEdit);
        assertTrue(duringEdit.contains("SAVE"), duringEdit);
        final JsonNode saved = OBJECT_MAPPER.readTree(providerState.resolve(connectorId).resolve("live-config.json").toFile());
        assertEquals("edited", propertyValue(saved, "Comment"));
        assertNull(propertyValue(saved, LEGACY_NOTE));

        createGates();
        final int journalBeforeStart = journal().size();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, org.apache.nifi.components.connector.ConnectorState.RUNNING);
        assertFalse(journalSince(journalBeforeStart).contains("SAVE"));
        assertEquals("edited", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());
    }

    @Test
    @Timeout(240)
    public void testDiscardedLiveConfigurationCannotReintroduceV1Properties() throws Exception {
        final Map<String, String> initial = properties("DRAIN", false, true, "shared");
        initial.put(LEGACY_NOTE, "v1-only");
        final String connectorId = createConnector("UpgradeDrainConnector", initial);
        getNiFiInstance().stop();
        final Path connectorDirectory = providerState.resolve(connectorId);
        Files.move(connectorDirectory.resolve("live-config.json"), connectorDirectory.resolve("committed-config.json"), StandardCopyOption.REPLACE_EXISTING);
        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();

        final int journalBeforeRead = journal().size();
        final ConnectorEntity upgraded = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertNull(configurationValue(upgraded.getComponent().getWorkingConfiguration(), LEGACY_NOTE));
        assertEquals("upgraded", configurationValue(upgraded.getComponent().getWorkingConfiguration(), DRAIN_LABEL));
        assertEquals("v1-only", propertyValue(OBJECT_MAPPER.readTree(connectorDirectory.resolve("committed-config.json").toFile()), LEGACY_NOTE));
        assertFalse(journalSince(journalBeforeRead).contains("SAVE"));

        getClientUtil().configureConnector(connectorId, "Upgrade", properties("DRAIN", false, true, "edited"));
        final JsonNode live = OBJECT_MAPPER.readTree(connectorDirectory.resolve("live-config.json").toFile());
        assertEquals("edited", propertyValue(live, "Comment"));
        assertNull(propertyValue(live, LEGACY_NOTE));

        final ConnectorEntity edited = getNifiClient().getConnectorClient().getConnector(connectorId);
        edited.setDisconnectedNodeAcknowledged(true);
        getNifiClient().getConnectorClient().discardConnectorUpdate(edited);
        assertFalse(Files.exists(connectorDirectory.resolve("live-config.json")));

        final int journalBeforeSecondRead = journal().size();
        final ConnectorEntity reloaded = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertNull(configurationValue(reloaded.getComponent().getWorkingConfiguration(), LEGACY_NOTE));
        assertEquals("upgraded", configurationValue(reloaded.getComponent().getWorkingConfiguration(), DRAIN_LABEL));
        assertEquals("shared", configurationValue(reloaded.getComponent().getWorkingConfiguration(), "Comment"));
        assertFalse(journalSince(journalBeforeSecondRead).contains("SAVE"));
        assertEquals("v1-only", propertyValue(OBJECT_MAPPER.readTree(connectorDirectory.resolve("committed-config.json").toFile()), LEGACY_NOTE));

        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, org.apache.nifi.components.connector.ConnectorState.RUNNING);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
    }

    @Test
    @Timeout(300)
    public void testExternalEditRequiresProviderSynchronizationBeforeStart() throws Exception {
        final String unchanged = createConnector("UpgradeDrainConnector", properties("DRAIN", false, true, "already-synchronized"));
        final String refreshed = createConnector("UpgradeDrainConnector", properties("DRAIN", false, true, "before-external-edit"));
        swapAndRestart();
        final ConnectorEntity unchangedEntity = getNifiClient().getConnectorClient().getConnector(unchanged);
        final int journalBeforeExternalEdit = journal().size();
        replaceComment(unchanged, "external-without-get");

        getNifiClient().getConnectorClient().startConnector(unchanged, unchangedEntity.getRevision().getClientId(), unchangedEntity.getRevision().getVersion());
        final String duringUnsynchronizedStart = journalSince(journalBeforeExternalEdit);
        assertFalse(duringUnsynchronizedStart.contains("LOAD"), duringUnsynchronizedStart);
        getClientUtil().waitForConnectorState(unchanged, org.apache.nifi.components.connector.ConnectorState.RUNNING);
        assertEquals("already-synchronized", findProcessor(unchanged, "GenerateFlowFile").getComponent().getConfig().getComments());

        replaceComment(refreshed, "external-after-get");
        final ConnectorEntity refreshedEntity = getNifiClient().getConnectorClient().getConnector(refreshed);
        assertEquals("external-after-get", configurationValue(refreshedEntity.getComponent().getWorkingConfiguration(), "Comment"));
        assertEquals("before-external-edit", configurationValue(refreshedEntity.getComponent().getActiveConfiguration(), "Comment"));
        assertManagedFlowBundle(org.apache.nifi.tests.system.FlowDefinitionFiles.readConnector(firstNode(), refreshed), V1_BUNDLE);
        getNifiClient().getConnectorClient().startConnector(refreshedEntity);
        getClientUtil().waitForConnectorState(refreshed, org.apache.nifi.components.connector.ConnectorState.RUNNING);
        assertEquals("external-after-get", findProcessor(refreshed, "GenerateFlowFile").getComponent().getConfig().getComments());
        getNiFiInstance().stop();
        final org.apache.nifi.flow.VersionedConnector afterEvolution = org.apache.nifi.tests.system.FlowDefinitionFiles.readConnector(firstNode(), refreshed);
        assertLoadedBundle(afterEvolution, V2_BUNDLE);
        assertManagedFlowBundle(afterEvolution, V2_BUNDLE);
    }

    @Test
    @Timeout(300)
    public void testExternalProviderAcrossVersionChange() throws Exception {
        uploadDrainNars(DRAIN_V1, DRAIN_V2);
        final Map<String, String> initial = properties("DRAIN", false, true, "before-external-edit");
        initial.put(LEGACY_NOTE, "v1-only");
        final String connectorId = queueStopped(createDrainConnector(DRAIN_V1, initial));

        changeVersion(connectorId, DRAIN_V2);
        assertPending(connectorId);
        final ConnectorEntity pending = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertNull(configurationValue(pending.getComponent().getWorkingConfiguration(), LEGACY_NOTE));
        assertEquals("upgraded", configurationValue(pending.getComponent().getWorkingConfiguration(), DRAIN_LABEL));

        replaceComment(connectorId, "external-after-get");
        final ConnectorEntity refreshedEntity = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertEquals("external-after-get", configurationValue(refreshedEntity.getComponent().getWorkingConfiguration(), "Comment"));
        assertEquals("before-external-edit", configurationValue(refreshedEntity.getComponent().getActiveConfiguration(), "Comment"));

        createGates();
        getNifiClient().getConnectorClient().startConnector(refreshedEntity);
        getClientUtil().waitForConnectorState(connectorId, org.apache.nifi.components.connector.ConnectorState.RUNNING);
        assertEquals("external-after-get", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));

        getNiFiInstance().stop();
        final ExpectedBundle versionTwo = new ExpectedBundle(DRAIN_BUNDLE_ARTIFACT, DRAIN_V2);
        final org.apache.nifi.flow.VersionedConnector afterEvolution = org.apache.nifi.tests.system.FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(afterEvolution, versionTwo);
        assertManagedFlowBundle(afterEvolution, versionTwo);
    }

    private void replaceComment(final String connectorId, final String comment) throws IOException {
        final Path live = providerState.resolve(connectorId).resolve("live-config.json");
        final JsonNode configuration = OBJECT_MAPPER.readTree(live.toFile());
        final JsonNode properties = configuration.get("workingFlowConfiguration").get(0).get("properties");
        ((com.fasterxml.jackson.databind.node.ObjectNode) properties.get("Comment")).put("value", comment);
        OBJECT_MAPPER.writeValue(live.toFile(), configuration);
    }

    private List<String> journal() throws IOException {
        final Path journal = providerState.resolve("journal.txt");
        if (!Files.exists(journal)) {
            return List.of();
        }
        return Files.readAllLines(journal);
    }

    private String journalSince(final int startingLine) throws IOException {
        final List<String> lines = journal();
        return String.join("\n", lines.subList(Math.min(startingLine, lines.size()), lines.size()));
    }

    private static String configurationValue(final ConnectorConfigurationDTO configuration, final String propertyName) {
        if (configuration == null || configuration.getConfigurationStepConfigurations() == null) {
            return null;
        }
        for (final ConfigurationStepConfigurationDTO step : configuration.getConfigurationStepConfigurations()) {
            if (!"Upgrade".equals(step.getConfigurationStepName()) || step.getPropertyGroupConfigurations() == null) {
                continue;
            }
            for (final PropertyGroupConfigurationDTO group : step.getPropertyGroupConfigurations()) {
                if (group.getPropertyValues() != null && group.getPropertyValues().containsKey(propertyName)) {
                    return group.getPropertyValues().get(propertyName).getValue();
                }
            }
        }
        return null;
    }

    private static String propertyValue(final JsonNode configuration, final String propertyName) {
        final JsonNode steps = configuration.get("workingFlowConfiguration");
        if (steps == null) {
            return null;
        }
        for (final JsonNode step : steps) {
            final JsonNode property = step.get("properties") == null ? null : step.get("properties").get(propertyName);
            if (property != null && !property.isNull()) {
                return property.get("value").asText();
            }
        }
        return null;
    }

    static final class OnlyProviderTests implements ExecutionCondition {
        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
            if (context.getTestMethod().isPresent() && context.getTestMethod().get().getDeclaringClass() != ConnectorUpgradeDrainProviderIT.class) {
                return ConditionEvaluationResult.disabled("Inherited upgrade-drain tests run from ConnectorUpgradeDrainIT");
            }
            return ConditionEvaluationResult.enabled("provider test");
        }
    }

    private static Path createProviderState() {
        try {
            return Files.createTempDirectory("recording-connector-provider");
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to create the provider state directory", e);
        }
    }
}
