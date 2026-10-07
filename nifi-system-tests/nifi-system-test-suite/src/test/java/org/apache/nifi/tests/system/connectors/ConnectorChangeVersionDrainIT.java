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

import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.flow.VersionedConnector;
import org.apache.nifi.tests.system.FlowDefinitionFiles;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.connectors.ChangeVersionConnector;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Single-node upgrade-drain scenarios for a version change. The version-change trigger, the drain timeout, and the
 * rollback tests run from {@link AbstractChangeVersionDrainIT}, including on a two-node cluster.
 */
public class ConnectorChangeVersionDrainIT extends AbstractChangeVersionDrainIT {

    private static final String LEGACY_NOTE = "Legacy Note";
    private static final String DRAIN_LABEL = "Drain Label";

    @Test
    @Timeout(240)
    public void testChangeBackToOutgoingVersionClearsPending() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);

        changeVersion(connectorId, DRAIN_V2);
        assertPending(connectorId);
        changeVersion(connectorId, DRAIN_V1);

        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertEquals(DRAIN_V1, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getBundle().getVersion());
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        getClientUtil().applyConnectorUpdate(getNifiClient().getConnectorClient().getConnector(connectorId));
        getClientUtil().waitForValidConnector(connectorId);

        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertFalse(recordExists());
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));

        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, outgoingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
    }

    @Test
    @Timeout(240)
    public void testChangeVersionTwiceKeepsOriginalOutgoingVersion() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        changeVersion(connectorId, DRAIN_V2);
        assertPending(connectorId);
        changeVersion(connectorId, DRAIN_V3);
        assertPending(connectorId);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));

        final ExpectedBundle versionThree = new ExpectedBundle(DRAIN_BUNDLE_ARTIFACT, DRAIN_V3);
        getNiFiInstance().stop();
        VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, versionThree);
        assertManagedFlowBundle(persisted, outgoingBundle());
        getNiFiInstance().start(true);
        waitForCluster();

        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        final String record = readRecord();
        assertTrue(record.startsWith("pre=" + outgoingBundle().recordPrefix()), record);
        assertTrue(record.contains("post=" + versionThree.recordPrefix()), record);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));

        getNiFiInstance().stop();
        persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, versionThree);
        assertManagedFlowBundle(persisted, versionThree);
    }

    @Test
    @Timeout(300)
    public void testGhostRestartKeepsQueuedFlowFiles() throws Exception {
        uploadDrainNars(DRAIN_UNLOADABLE_VERSION);
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);

        changeVersion(connectorId, DRAIN_UNLOADABLE_VERSION);
        assertEquals(Boolean.TRUE, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getExtensionMissing());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));

        restartKeepingIncomingVersion();
        assertEquals(Boolean.TRUE, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getExtensionMissing());
        assertConnectorState(connectorId, ConnectorState.STOPPED);
        final Map<String, String> processorStatesWhileGhosted = managedProcessorStates(connectorId);
        assertNotNull(actionReason(connectorId, "START"));
        assertThrows(NiFiClientException.class, () -> requestStart(connectorId));
        assertFalse(observedDraining(connectorId));
        assertFalse(recordExists());
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertAllStopped(processorStatesWhileGhosted);

        getNiFiInstance().stop();
        final VersionedConnector whileGhosted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(whileGhosted, new ExpectedBundle(DRAIN_BUNDLE_ARTIFACT, DRAIN_UNLOADABLE_VERSION));
        assertManagedFlowBundle(whileGhosted, outgoingBundle());
        assertTrue(whileGhosted.getManagedProcessGroup().getConnections().stream()
            .anyMatch(connection -> connectionId.equals(connection.getInstanceIdentifier()) || REMOVED_CONNECTION.equals(connection.getName())));
        getNiFiInstance().start(true);
        waitForCluster();

        changeVersion(connectorId, DRAIN_V2);
        assertEquals(Boolean.FALSE, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getExtensionMissing());
        assertPending(connectorId);
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        final int queuedOnceLoadable = getConnectorQueuedFlowFileCount(connectorId);

        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        final String record = readRecord();
        assertTrue(record.startsWith("pre=" + outgoingBundle().recordPrefix()), record);
        assertTrue(record.contains("post=" + incomingBundle().recordPrefix()), record);

        final Map<File, Long> logSizes = appLogSizes();
        restartKeepingIncomingVersion();
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        final String startupLog = appLogSince(logSizes);
        assertFalse(startupLog.contains("Queue identifier " + connectionId + " but no Queue exists"),
            "FlowFiles queued in connection " + connectionId + " were not recovered after the Connector became loadable, evolved and restarted: "
                + "the connection no longer exists and the FlowFile repository still holds them without a queue. Queue count once loadable was "
                + queuedOnceLoadable + " of " + queued);

        assertEquals(queued, queuedOnceLoadable);
        assertAllStopped(processorStatesWhileGhosted);
    }

    @Test
    @Timeout(240)
    public void testChangeGhostToInstalledVersionRestoresQueues() throws Exception {
        uploadDrainNars(DRAIN_UNLOADABLE_VERSION);
        final String connectorId = queueStoppedUpgrade("DRAIN", true, "original");
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        changeVersion(connectorId, DRAIN_UNLOADABLE_VERSION);
        assertEquals(Boolean.TRUE, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getExtensionMissing());

        changeVersion(connectorId, DRAIN_V2);
        final ConnectorEntity installed = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertEquals(Boolean.FALSE, installed.getComponent().getExtensionMissing());
        assertEquals(DRAIN_V2, installed.getComponent().getBundle().getVersion());
        assertEquals("original", configurationValue(installed.getComponent().getActiveConfiguration(), "Comment"));
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertPending(connectorId);

        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        final String record = readRecord();
        assertTrue(record.startsWith("pre=" + outgoingBundle().recordPrefix()), record);
        assertTrue(record.contains("post=" + incomingBundle().recordPrefix()), record);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertEquals("original", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());
    }

    @Test
    @Timeout(240)
    public void testChangeVersionRejectedInTroubleshootingAndWhileDraining() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        getClientUtil().enterTroubleshooting(connectorId);
        assertNotNull(actionReason(connectorId, "CHANGE_VERSION"));
        assertThrows(NiFiClientException.class, () -> changeVersion(connectorId, DRAIN_V2));
        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertEquals(DRAIN_V1, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getBundle().getVersion());

        changeVersion(connectorId, DRAIN_V2);
        deleteGates();
        requestStart(connectorId);
        waitUntilDraining(connectorId);
        assertNotNull(actionReason(connectorId, "CHANGE_VERSION"));
        assertThrows(NiFiClientException.class, () -> changeVersion(connectorId, DRAIN_V3));
        assertConnectorState(connectorId, ConnectorState.DRAINING);
        assertEquals(DRAIN_V2, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getBundle().getVersion());

        getClientUtil().cancelDrain(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);
        assertPending(connectorId);
        assertNull(actionReason(connectorId, "CHANGE_VERSION"));
    }

    @Test
    @Timeout(240)
    public void testCliStopChangeStartDrains() throws Exception {
        final String connectorId = createOutgoingConnector("DRAIN", true, null);
        deleteGates();
        getClientUtil().startConnector(connectorId);
        waitForConnectorMinQueueCount(connectorId, 1);

        final Properties properties = new Properties();
        properties.setProperty(CommandOption.EXT_BUNDLE_GROUP.getLongName(), DRAIN_BUNDLE_GROUP);
        properties.setProperty(CommandOption.EXT_BUNDLE_ARTIFACT.getLongName(), DRAIN_BUNDLE_ARTIFACT);
        properties.setProperty(CommandOption.EXT_BUNDLE_VERSION.getLongName(), DRAIN_V2);
        properties.setProperty(CommandOption.EXT_QUALIFIED_NAME.getLongName(), DRAIN_CONNECTOR_TYPE);
        properties.setProperty(CommandOption.EXT_BUNDLE_CURRENT_VERSION.getLongName(), DRAIN_V1);
        new ChangeVersionConnector().doExecute(getNifiClient(), properties);

        waitUntilDraining(connectorId);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));
        createGates();
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertEquals(DRAIN_V2, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getBundle().getVersion());
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        final String record = readRecord();
        assertTrue(record.startsWith("pre=" + outgoingBundle().recordPrefix()), record);
        assertTrue(record.contains("post=" + incomingBundle().recordPrefix()), record);
    }

    @Test
    @Timeout(240)
    public void testMigratedActiveConfigurationPersistedWhilePending() throws Exception {
        final Map<String, String> properties = properties("DRAIN", false, true, "shared");
        properties.put(LEGACY_NOTE, "v1-only");
        final String connectorId = queueStopped(createOutgoingConnector(properties));

        changeVersion(connectorId, DRAIN_V2);
        assertPending(connectorId);
        final ConnectorEntity pending = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertNull(configurationValue(pending.getComponent().getActiveConfiguration(), LEGACY_NOTE));
        assertEquals("upgraded", configurationValue(pending.getComponent().getActiveConfiguration(), DRAIN_LABEL));
        assertEquals("shared", configurationValue(pending.getComponent().getActiveConfiguration(), "Comment"));

        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
        assertNull(configurationProperty(persisted.getActiveFlowConfiguration(), LEGACY_NOTE));
        assertEquals("shared", configurationProperty(persisted.getActiveFlowConfiguration(), "Comment"));
        getNiFiInstance().start(true);
        waitForCluster();

        assertPending(connectorId);
        final ConnectorEntity restarted = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertNull(configurationValue(restarted.getComponent().getActiveConfiguration(), LEGACY_NOTE));
        assertEquals("upgraded", configurationValue(restarted.getComponent().getActiveConfiguration(), DRAIN_LABEL));
        assertEquals("shared", configurationValue(restarted.getComponent().getActiveConfiguration(), "Comment"));
    }

    @Test
    @Timeout(300)
    public void testCompletedUpgradeThenVersionChangeBeforeStart() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", false, null);
        changeVersion(connectorId, DRAIN_V2);
        assertPending(connectorId);
        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        getClientUtil().stopConnector(connectorId);
        deleteRecords();

        changeVersion(connectorId, DRAIN_V3);
        assertPending(connectorId);
        final ExpectedBundle versionThree = new ExpectedBundle(DRAIN_BUNDLE_ARTIFACT, DRAIN_V3);
        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, versionThree);
        assertManagedFlowBundle(persisted, incomingBundle());
        getNiFiInstance().start(true);
        waitForCluster();

        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        final String record = readRecord();
        assertTrue(record.startsWith("pre=" + incomingBundle().recordPrefix()), record);
        assertTrue(record.contains("post=" + versionThree.recordPrefix()), record);
    }

    @Test
    @Timeout(240)
    public void testNestedGroupDrainReachesRunning() throws Exception {
        // INHERITED_DRAIN is the mode that calls AbstractConnector.drainFlowFiles. DRAIN starts only the root
        // processor named TerminateFlowFile, so it never double-starts the nested processor.
        final Map<String, String> properties = properties("INHERITED_DRAIN", false, true, null);
        properties.put("Nested Group", "true");
        final String connectorId = createOutgoingConnector(properties);
        deleteGates();
        getClientUtil().startConnector(connectorId);
        waitForConnectorMinQueueCount(connectorId, 2);
        getClientUtil().stopConnector(connectorId);
        triggerUpgrade(connectorId);
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) > 0);

        requestStart(connectorId);
        waitUntilDraining(connectorId);
        createGates();
        waitForRetryAfterQueueDrained(connectorId);

        assertTrue(recordExists());
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
    }

    @Test
    @Timeout(240)
    public void testHookFailureStaysPending() throws Exception {
        final String connectorId = queueStoppedUpgrade("FAIL", true, null);
        triggerUpgrade(connectorId);
        createFailMarkers();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);
        final ConnectorEntity failed = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertTrue(failed.getComponent().getValidationErrors().toString().contains("evolution failed"));
        assertPending(connectorId);
        final long bulletinDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        List<BulletinEntity> bulletins = List.of();
        boolean evolutionBulletin = false;
        while (System.nanoTime() < bulletinDeadline) {
            bulletins = getNifiClient().getConnectorClient().getConnector(connectorId).getBulletins();
            evolutionBulletin = bulletins != null && bulletins.stream()
                .anyMatch(bulletin -> bulletin.getBulletin() != null && bulletin.getBulletin().getMessage().contains("evolution failed"));
            if (evolutionBulletin) {
                break;
            }
            Thread.sleep(200L);
        }
        assertTrue(evolutionBulletin, "Expected an evolution failure bulletin but found " + bulletins);

        restartKeepingIncomingVersion();
        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertPending(connectorId);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));

        deleteFailMarkers();
        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
    }

    @Test
    @Timeout(240)
    public void testNoOpHookWithNoRemovedConnection() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, null);
        triggerUpgrade(connectorId);
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));
    }

    @Test
    @Timeout(240)
    public void testNoOpHookWithQueuedDataFails() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", true, null);
        triggerUpgrade(connectorId);
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);
        final String errors = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getValidationErrors().toString();
        assertTrue(errors.contains(REMOVED_CONNECTION), errors);
        assertTrue(errors.contains("GenerateFlowFile to TerminateFlowFile"), errors);
        assertTrue(errors.contains("FlowFiles"), errors);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertPending(connectorId);
    }

    @Test
    @Timeout(240)
    public void testInheritedDrainTimeoutThenRetryLeavesDraining() throws Exception {
        getNiFiInstance().setProperty(NiFiProperties.CONNECTOR_FLOW_EVOLUTION_TIMEOUT, "5 secs");
        applyInstanceProperties();
        deleteEvolutionPhaseRecords();
        final String connectorId = queueStoppedUpgrade("INHERITED_DRAIN", true, null);
        triggerUpgrade(connectorId);
        deleteGates();
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) > 0);

        requestStart(connectorId);
        waitUntilDraining(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);
        assertPending(connectorId);
        assertFalse(phaseRecordExists(PHASE_ONE_COMPLETE_RECORD_FILE));
        assertFalse(phaseRecordExists(PHASE_TWO_RECORD_FILE));

        createGates();
        requestStart(connectorId);
        waitForRetryAfterQueueDrained(connectorId);
        assertTrue(phaseRecordExists(PHASE_ONE_COMPLETE_RECORD_FILE));
        assertTrue(phaseRecordExists(PHASE_TWO_RECORD_FILE));
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));

        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
        assertManagedFlowBundle(persisted, incomingBundle());
    }

    static String configurationValue(final ConnectorConfigurationDTO configuration, final String propertyName) {
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
}
