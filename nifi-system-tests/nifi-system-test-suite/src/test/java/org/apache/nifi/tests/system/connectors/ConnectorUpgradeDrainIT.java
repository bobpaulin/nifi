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
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.tests.system.FlowDefinitionFiles;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Single-node upgrade-drain scenarios that only a NAR swap can reach.
 */
public class ConnectorUpgradeDrainIT extends AbstractNarSwapUpgradeDrainIT {

    @Test
    @Timeout(240)
    public void testBundlesMatchRestoresPersistedFlow() throws Exception {
        final String connectorId = createUpgradeConnector("DRAIN", false, true, "original");
        deleteGates();
        getClientUtil().startConnector(connectorId);
        waitForConnectorMinQueueCount(connectorId, 1);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final String generateId = findProcessor(connectorId, "GenerateFlowFile").getId();
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();

        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, V1_BUNDLE);
        assertManagedFlowBundle(persisted, V1_BUNDLE);
        getNiFiInstance().start(true);
        waitForCluster();

        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertFalse(observedDraining(connectorId));
        assertEquals(generateId, findProcessor(connectorId, "GenerateFlowFile").getId());
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) >= queued);
        getClientUtil().applyConnectorUpdate(getNifiClient().getConnectorClient().getConnector(connectorId));
        assertFalse(recordExists());
    }

    @Test
    @Timeout(240)
    public void testPersistedProcessorBundleResolvedOnUpgrade() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", false, true, null);
        getNiFiInstance().stop();
        FlowDefinitionFiles.rewriteManagedComponentBundleVersion(getNiFiInstance(), connectorId, "0.0.1");
        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();

        // Terminate drops queued FlowFiles only after the gate file exists.
        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        final ConnectorEntity running = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertFalse(String.valueOf(running.getComponent().getValidationErrors()).contains("missing"),
            String.valueOf(running.getComponent().getValidationErrors()));
        final String record = readRecord();
        assertTrue(record.contains("pre=org.apache.nifi:" + V1_BUNDLE + ":"), record);
        assertTrue(record.contains("post=org.apache.nifi:" + V2_BUNDLE + ":"), record);

        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertInstalledComponentBundles(persisted.getManagedProcessGroup());
        assertManagedFlowBundle(persisted, V2_BUNDLE);
    }

    private static void assertInstalledComponentBundles(final VersionedProcessGroup group) {
        if (group == null) {
            return;
        }
        if (group.getProcessors() != null) {
            for (final VersionedProcessor processor : group.getProcessors()) {
                assertNotNull(processor.getBundle());
                assertFalse("0.0.1".equals(processor.getBundle().getVersion()), processor.getName());
            }
        }
        if (group.getControllerServices() != null) {
            for (final VersionedControllerService service : group.getControllerServices()) {
                assertNotNull(service.getBundle());
                assertFalse("0.0.1".equals(service.getBundle().getVersion()), service.getName());
            }
        }
        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
                assertInstalledComponentBundles(childGroup);
            }
        }
    }

    @Test
    @Timeout(240)
    public void testMissingManagedFlowBundleIsInferredOnUpgrade() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, "original");
        getNiFiInstance().stop();
        FlowDefinitionFiles.stripManagedFlowBundle(getNiFiInstance(), connectorId);
        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();

        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertPending(connectorId);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));
        getNiFiInstance().stop();
        VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
        getNiFiInstance().start(true);
        waitForCluster();

        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        final String record = readRecord();
        assertTrue(record.contains(outgoingBundle().recordPrefix()), record);
        assertTrue(record.contains(incomingBundle().recordPrefix()), record);
        getNiFiInstance().stop();
        persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
        assertManagedFlowBundle(persisted, incomingBundle());
    }

    @Test
    @Timeout(240)
    public void testNoPersistedFlowLoadsUpgradedFlow() throws Exception {
        final String connectorId = createUpgradeConnector("DRAIN", false, true, null);
        getNiFiInstance().stop();
        FlowDefinitionFiles.stripManagedProcessGroup(getNiFiInstance(), connectorId);
        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();

        getClientUtil().waitForValidConnector(connectorId);
        assertFalse(observedDraining(connectorId));
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertFalse(recordExists());
        getClientUtil().startConnector(connectorId);
        assertConnectorState(connectorId, ConnectorState.RUNNING);
        assertFalse(recordExists());

        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, V2_BUNDLE);
        assertManagedFlowBundle(persisted, V2_BUNDLE);
        assertNotNull(persisted.getManagedProcessGroup());
        assertNull(findProcessor(persisted.getManagedProcessGroup(), "TerminateFlowFile"));
    }

    @Test
    @Timeout(300)
    public void testGhostedRunningConnectorKeepsQueueWithoutResuming() throws Exception {
        final String connectorId = createConnector("UpgradeOnlyV1Connector", Map.of("Gate File Path", GATE_FILE));
        deleteGates();
        getClientUtil().startConnector(connectorId);
        waitForConnectorMinQueueCount(connectorId, 1);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);

        getNiFiInstance().stop();
        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();

        getClientUtil().waitForInvalidConnector(connectorId);
        assertThrows(NiFiClientException.class, () -> requestStart(connectorId));

        // Checkpoint: ghost restart installs the persisted flow without resuming components, so queues keep their FlowFiles.
        assertNotNull(findProcessor(connectorId, "GenerateFlowFile"), "GenerateFlowFile was not installed after ghost restart");
        assertAllStopped(managedProcessorStates(connectorId));
        final int afterRestart = getConnectorQueuedFlowFileCount(connectorId);
        assertTrue(afterRestart >= queued, "queued count after ghost restart was " + afterRestart + ", expected at least " + queued);
        Thread.sleep(3_000L);
        assertEquals(afterRestart, getConnectorQueuedFlowFileCount(connectorId), "queue grew while ghosted; components were resumed");

        getNiFiInstance().stop();
        switchNarsBack();
        getNiFiInstance().start(true);
        waitForCluster();
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) >= afterRestart);
    }

    @Test
    @Timeout(240)
    public void testGhostedConnectorPreservesPersistedFlow() throws Exception {
        final String connectorId = createConnector("UpgradeOnlyV1Connector", Map.of("Gate File Path", GATE_FILE));
        deleteGates();
        getClientUtil().startConnector(connectorId);
        waitForConnectorMinQueueCount(connectorId, 1);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final String generateId = findProcessor(connectorId, "GenerateFlowFile").getId();
        getClientUtil().stopConnector(connectorId);

        getNiFiInstance().stop();
        final VersionedConnector before = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();

        getClientUtil().waitForInvalidConnector(connectorId);
        final ConnectorEntity ghost = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);
        final String ghostErrors = ghost.getComponent().getValidationErrors().toString();
        assertTrue(ghostErrors.contains("UpgradeOnlyV1Connector"), ghostErrors);
        assertThrows(NiFiClientException.class, () -> requestStart(connectorId));
        assertFalse(observedDraining(connectorId));
        final String enterReason = actionReason(connectorId, "ENTER_TROUBLESHOOTING");
        assertNotNull(enterReason);
        assertTrue(enterReason.toLowerCase().contains("missing") || enterReason.toLowerCase().contains("ghost"));
        assertEquals(generateId, findProcessor(connectorId, "GenerateFlowFile").getId());
        assertAllStopped(managedProcessorStates(connectorId));
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) >= queued);

        getNiFiInstance().stop();
        getNiFiInstance().start(true);
        waitForCluster();
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) >= queued);
        getNiFiInstance().stop();
        final VersionedConnector whileGhosted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertEquals(before.getBundle().getArtifact(), whileGhosted.getBundle().getArtifact());
        assertEquals(before.getBundle().getVersion(), whileGhosted.getBundle().getVersion());
        assertNotNull(whileGhosted.getManagedFlowBundle());
        assertEquals(before.getManagedFlowBundle().getArtifact(), whileGhosted.getManagedFlowBundle().getArtifact());
        assertEquals(before.getManagedFlowBundle().getVersion(), whileGhosted.getManagedFlowBundle().getVersion());
        assertNotNull(whileGhosted.getManagedProcessGroup());
        assertNotNull(findProcessor(whileGhosted.getManagedProcessGroup(), "GenerateFlowFile"));

        switchNarsBack();
        getNiFiInstance().start(true);
        waitForCluster();
        assertEquals(generateId, findProcessor(connectorId, "GenerateFlowFile").getId());
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) >= queued);
        getClientUtil().startConnector(connectorId);
        assertConnectorState(connectorId, ConnectorState.RUNNING);
        assertFalse(recordExists());
        getClientUtil().applyConnectorUpdate(getNifiClient().getConnectorClient().getConnector(connectorId));
    }

    @Test
    @Timeout(240)
    public void testGhostedComponentsBlockEvolution() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, true, null);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        swapAndRestart();

        final NiFiClientException rejected = assertThrows(NiFiClientException.class, () -> requestStart(connectorId));
        assertTrue(rejected.getMessage().contains("UpgradeOnlyV1"), rejected.getMessage());
        assertFalse(observedDraining(connectorId));
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertPending(connectorId);
    }

    @Test
    @Timeout(300)
    public void testUpgradeWhileInTroubleshooting() throws Exception {
        final String connectorId = createUpgradeConnector("DRAIN", false, true, null);
        deleteGates();
        getClientUtil().startConnector(connectorId);
        waitForConnectorMinQueueCount(connectorId, 1);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        getClientUtil().stopConnector(connectorId);
        getClientUtil().enterTroubleshooting(connectorId);
        getClientUtil().updateProcessorSchedulingPeriod(findProcessor(connectorId, "GenerateFlowFile"), "1 hour");

        getNiFiInstance().stop();
        VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertEquals("TROUBLESHOOTING", persisted.getScheduledState().name());
        assertLoadedBundle(persisted, V1_BUNDLE);
        assertManagedFlowBundle(persisted, V1_BUNDLE);
        assertEquals("1 hour", findProcessor(persisted.getManagedProcessGroup(), "GenerateFlowFile").getSchedulingPeriod());

        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);
        assertEquals("1 hour", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getSchedulingPeriod());
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) >= queued);
        assertFalse(recordExists());

        getNiFiInstance().stop();
        getNiFiInstance().start(true);
        waitForCluster();
        getNiFiInstance().stop();
        persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, V2_BUNDLE);
        assertManagedFlowBundle(persisted, V1_BUNDLE);
        assertEquals("1 hour", findProcessor(persisted.getManagedProcessGroup(), "GenerateFlowFile").getSchedulingPeriod());
        getNiFiInstance().start(true);
        waitForCluster();

        stopManagedProcessors(connectorId);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final NiFiClientException rejected = assertThrows(NiFiClientException.class, () -> getClientUtil().endTroubleshooting(connectorId));
        assertTrue(rejected.getMessage().contains("upgrade is pending"), rejected.getMessage());
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        getClientUtil().emptyQueue(connectionId);
        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertFalse(recordExists());
        getNiFiInstance().stop();
        persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, V2_BUNDLE);
        assertManagedFlowBundle(persisted, V2_BUNDLE);
        getNiFiInstance().start(true);
        waitForCluster();

        createGates();
        getClientUtil().startConnector(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertFalse(recordExists());
    }

}
