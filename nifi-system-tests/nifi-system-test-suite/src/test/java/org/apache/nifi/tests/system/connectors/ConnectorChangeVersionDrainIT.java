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
import org.apache.nifi.flow.VersionedConnectorState;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.tests.system.FlowDefinitionFiles;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Single-node upgrade-drain scenarios for a version change.
 */
public class ConnectorChangeVersionDrainIT extends AbstractChangeVersionDrainIT {

    private static final String UPGRADE_SECRET = "Upgrade Secret";
    private static final String PROVIDER_NAME = "PropertiesParameterProvider";
    private static final String SECRET_NAME = "upgrade-secret";
    private static final String SECRET_FULLY_QUALIFIED_NAME = PROVIDER_NAME + ".Parameters." + SECRET_NAME;
    private static final String MARKER_SERVICE = "Marker Service";
    private static final String GENERATOR = "GenerateFlowFile";

    @Test
    @Timeout(360)
    public void testPreUpgradeParameterSecretIsRefreshedAndCanBeRestored() throws Exception {
        final SecretConnector secretConnector = createSecretConnector("original-secret");
        changeVersion(secretConnector.connectorId(), DRAIN_V2);
        getClientUtil().updateParameterProviderProperties(secretConnector.parameterProvider(), Map.of("parameters", SECRET_NAME + "=rotated-secret"));

        restartKeepingIncomingVersion();
        requestStart(secretConnector.connectorId());
        getClientUtil().waitForConnectorState(secretConnector.connectorId(), ConnectorState.RUNNING);
        stopAndAssertNotPending(secretConnector.connectorId());
    }

    @Test
    @Timeout(360)
    public void testPendingUpgradeEditsThenStartsWithoutReEvolving() throws Exception {
        final String connectorId = queueAtLeastOneFlowFileAndStop(createOutgoingConnector(markerProperties("INHERITED_DRAIN", true, "original")));
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final long generated = generatorCount(connectorId);
        assertQueueMatchesCount(connectorId);
        triggerUpgrade(connectorId);

        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertPending(connectorId);

        getClientUtil().configureConnector(connectorId, "Upgrade", markerProperties("DRAIN", true, "working-edit"));
        assertPending(connectorId);
        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
        assertNotNull(findProcessor(persisted.getManagedProcessGroup(), "TerminateFlowFile"));
        final String serviceId = findMarkerServiceId(persisted);
        getNiFiInstance().start(true);
        waitForCluster();

        final String marker = markerValue(connectorId, serviceId);
        assertNotNull(marker);
        assertEquals(generated, generatorCount(connectorId));
        assertQueueMatchesCount(connectorId);

        // Releasing FlowFiles stops the generator, so its count must not move while the pre-upgrade flow drains and evolves.
        releaseFlowFiles();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertEquals("working-edit", findProcessor(connectorId, GENERATOR).getComponent().getConfig().getComments());
        assertTrue(findProcessor(connectorId, GENERATOR).getComponent().getBundle().getArtifact().contains("nifi-system-test-extensions2"));
        assertEquals(generated, generatorCount(connectorId));
        assertEquals(marker, markerValue(connectorId, serviceId));

        getNiFiInstance().stop();
        final VersionedConnector evolved = FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId);
        assertManagedFlowBundle(evolved, incomingBundle());
        assertEquals(VersionedConnectorState.RUNNING, evolved.getScheduledState());
        assertEquals("working-edit", configurationProperty(evolved.getActiveFlowConfiguration(), "Comment"));
        assertEquals("working-edit", configurationProperty(evolved.getWorkingFlowConfiguration(), "Comment"));
        assertEquals("working-edit", findProcessor(evolved.getManagedProcessGroup(), "GenerateFlowFile").getComments());
        assertTrue(findProcessor(evolved.getManagedProcessGroup(), "GenerateFlowFile").getBundle().getArtifact().contains("nifi-system-test-extensions2"));
        getNiFiInstance().start(true);
        waitForCluster();
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);

        getClientUtil().stopConnector(connectorId);
        getClientUtil().configureConnector(connectorId, "Upgrade", markerProperties("DRAIN", true, "after-evolution"));
        getClientUtil().applyConnectorUpdate(getNifiClient().getConnectorClient().getConnector(connectorId));
        getClientUtil().waitForValidConnector(connectorId);
        assertEquals("after-evolution", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());
    }

    @Test
    @Timeout(360)
    public void testPendingUpgradeClearsWhenBundleMatchesManagedFlow() throws Exception {
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

        releaseFlowFiles();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));

        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId);
        assertLoadedBundle(persisted, outgoingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
    }

    @Test
    @Timeout(240)
    public void testRestartDuringDrainingComesBackPending() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        triggerUpgrade(connectorId);
        holdFlowFiles();
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) > 0);
        requestStart(connectorId);
        waitUntilDraining(connectorId);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final long generated = generatorCount(connectorId);
        restartKeepingIncomingVersion();
        getClientUtil().cancelDrain(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertEquals(generated, generatorCount(connectorId));
        assertQueueMatchesCount(connectorId);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertPending(connectorId);
        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
    }

    @Test
    @Timeout(240)
    public void testInvalidEvolutionResultLeavesUpgradePendingUntilValid() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, null);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final long generated = generatorCount(connectorId);
        assertQueueMatchesCount(connectorId);
        changeVersion(connectorId, DRAIN_V2);

        final Map<UpgradeDrainFault, String> expectedErrors = new LinkedHashMap<>();
        expectedErrors.put(UpgradeDrainFault.HOOK_RETURNS_NULL_FUTURE, "must return a CompletableFuture");
        expectedErrors.put(UpgradeDrainFault.HOOK_RETURNS_FAILED_FUTURE, "hook failed");
        expectedErrors.put(UpgradeDrainFault.ACTIVE_FLOW_RETURNS_NULL, "Upgraded flow was not returned");
        expectedErrors.put(UpgradeDrainFault.ACTIVE_FLOW_THROWS, "active flow failed");
        for (final Map.Entry<UpgradeDrainFault, String> expectedError : expectedErrors.entrySet()) {
            clearFaults();
            injectFault(expectedError.getKey());

            requestStart(connectorId);
            assertStoppedPending(connectorId, expectedError.getValue());
            assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
            assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
            assertEquals(generated, generatorCount(connectorId));
            assertPersistedOutgoing(connectorId);
        }

        // The upgraded flow keeps the queued connection. FlowFiles are still held, so the generator keeps running and its
        // count grows, but it must still equal the number of FlowFiles queued behind the terminator.
        clearFaults();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        getClientUtil().stopConnector(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);

        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertQueueMatchesCount(connectorId);
        assertTrue(generatorCount(connectorId) >= generated);
        stopAndAssertNotPending(connectorId);
    }

    @Test
    @Timeout(240)
    public void testQueueAndStateSurviveTroubleshootingRoundTrip() throws Exception {
        final String connectorId = queueAtLeastOneFlowFileAndStop(createOutgoingConnector(markerProperties("NO_OP", false, null)));
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final long generated = generatorCount(connectorId);
        assertQueueMatchesCount(connectorId);

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);
        final String serviceId = findMarkerServiceId(connectorId);
        final String marker = markerValue(connectorId, serviceId);
        assertNotNull(marker);
        assertQueueAndCountUnchanged(connectorId, connectionId, queued, generated);

        restartKeepingIncomingVersion();
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);
        assertQueueAndCountUnchanged(connectorId, connectionId, queued, generated);
        assertEquals(marker, markerValue(connectorId, serviceId));

        // Ending Troubleshooting requires every Controller Service in the managed flow to be disabled.
        final String managedGroupId = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getManagedProcessGroupId();
        getClientUtil().disableControllerServices(managedGroupId, true);
        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertQueueAndCountUnchanged(connectorId, connectionId, queued, generated);
        assertEquals(marker, markerValue(connectorId, serviceId));
    }

    private Map<String, String> markerProperties(final String mode, final boolean removeConnection, final String comment) {
        final Map<String, String> properties = properties(mode, removeConnection, comment);
        properties.put(MARKER_SERVICE, "true");
        return properties;
    }

    private void assertQueueAndCountUnchanged(final String connectorId, final String connectionId, final int queued, final long generated)
            throws NiFiClientException, IOException {
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertEquals(generated, generatorCount(connectorId));
    }

    /**
     * While FlowFiles are held, the terminator keeps every generated FlowFile, so the generator's stored count equals the
     * number of queued FlowFiles. Lost FlowFiles make the queue smaller; cleared state makes the count smaller.
     */
    private void assertQueueMatchesCount(final String connectorId) throws NiFiClientException, IOException {
        assertEquals(getConnectionQueuedCount(connectorId, REMOVED_CONNECTION), generatorCount(connectorId));
    }

    private long generatorCount(final String connectorId) throws NiFiClientException, IOException {
        final String generatorId = findProcessor(connectorId, GENERATOR).getId();
        final String count = localStateValue(getNifiClient().getConnectorClient().getProcessorState(connectorId, generatorId), "count");
        return count == null ? 0L : Long.parseLong(count);
    }

    private String markerValue(final String connectorId, final String serviceId) throws NiFiClientException, IOException {
        return localStateValue(getNifiClient().getConnectorClient().getControllerServiceState(connectorId, serviceId), "marker");
    }

    private static String localStateValue(final ComponentStateEntity stateEntity, final String key) {
        for (final StateEntryDTO entry : stateEntity.getComponentState().getLocalState().getState()) {
            if (key.equals(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private String findMarkerServiceId(final String connectorId) throws NiFiClientException, IOException {
        final String managedGroupId = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getManagedProcessGroupId();
        for (final ControllerServiceEntity service : getNifiClient().getFlowClient().getControllerServices(managedGroupId).getControllerServices()) {
            if (MARKER_SERVICE.equals(service.getComponent().getName())) {
                return service.getId();
            }
        }
        throw new AssertionError("Managed flow of " + connectorId + " has no " + MARKER_SERVICE);
    }

    private static String findMarkerServiceId(final VersionedConnector connector) {
        for (final VersionedControllerService service : connector.getManagedProcessGroup().getControllerServices()) {
            if (MARKER_SERVICE.equals(service.getName())) {
                return service.getInstanceIdentifier();
            }
        }
        throw new AssertionError("Persisted managed flow has no " + MARKER_SERVICE);
    }

    @Test
    @Timeout(240)
    public void testChangeVersionRejectedWhileDraining() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        changeVersion(connectorId, DRAIN_V2);
        holdFlowFiles();
        requestStart(connectorId);
        waitUntilDraining(connectorId);
        assertNotNull(actionReason(connectorId, "CHANGE_VERSION"));
        assertThrows(NiFiClientException.class, () -> changeVersion(connectorId, DRAIN_V1));
        assertConnectorState(connectorId, ConnectorState.DRAINING);
        assertEquals(DRAIN_V2, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getBundle().getVersion());

        getClientUtil().cancelDrain(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);
        assertPending(connectorId);
        assertNull(actionReason(connectorId, "CHANGE_VERSION"));
    }

    private SecretConnector createSecretConnector(final String secretValue) throws Exception {
        final ParameterProviderEntity parameterProvider = getClientUtil().createParameterProvider(PROVIDER_NAME);
        final ParameterProviderEntity configuredProvider = getClientUtil().updateParameterProviderProperties(
            parameterProvider, Map.of("parameters", SECRET_NAME + "=" + secretValue));
        final ConnectorEntity connector = getClientUtil().createConnector(
            DRAIN_CONNECTOR_TYPE, DRAIN_BUNDLE_GROUP, DRAIN_BUNDLE_ARTIFACT, DRAIN_V1);

        final Map<String, ConnectorValueReferenceDTO> propertyReferences = new HashMap<>();
        for (final Map.Entry<String, String> property : properties("NO_OP", false, null).entrySet()) {
            final ConnectorValueReferenceDTO reference = new ConnectorValueReferenceDTO();
            reference.setValueType("STRING_LITERAL");
            reference.setValue(property.getValue());
            propertyReferences.put(property.getKey(), reference);
        }
        propertyReferences.put(UPGRADE_SECRET, getClientUtil().createSecretValueReference(
            configuredProvider.getId(), SECRET_NAME, SECRET_FULLY_QUALIFIED_NAME));

        getClientUtil().configureConnectorWithReferences(connector, "Upgrade", propertyReferences);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());
        return new SecretConnector(connector.getId(), configuredProvider);
    }

    private void assertStoppedPending(final String connectorId, final String validationError) throws Exception {
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);
        assertPending(connectorId);
        if (validationError == null) {
            return;
        }
        final String errors = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getValidationErrors().toString();
        assertTrue(errors.contains(validationError), errors);
    }

    private record SecretConnector(String connectorId, ParameterProviderEntity parameterProvider) {
    }
}
