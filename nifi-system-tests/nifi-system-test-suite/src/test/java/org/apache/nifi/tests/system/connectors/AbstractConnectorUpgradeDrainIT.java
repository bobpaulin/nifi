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
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurationStep;
import org.apache.nifi.flow.VersionedConnector;
import org.apache.nifi.flow.VersionedConnectorState;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.nar.NarState;
import org.apache.nifi.tests.system.AbstractNarSwapMigrationIT;
import org.apache.nifi.tests.system.FlowDefinitionFiles;
import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.nar.NarUploadUtil;
import org.apache.nifi.toolkit.client.ConnectorClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectorActionDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Upgrade-drain scenarios that hold for every way a Connector can move to a new version. Each subclass supplies the
 * trigger: a NAR swap followed by a restart, or a version change while the Connector is stopped.
 */
public abstract class AbstractConnectorUpgradeDrainIT extends AbstractNarSwapMigrationIT {

    static final String GATE_FILE = "upgrade-drain-gate.txt";
    static final String FAIL_MARKER_FILE = "upgrade-drain-fail.txt";
    static final String APPLY_FAIL_FILE = "upgrade-drain-apply-fail.txt";
    static final String APPLY_BLOCK_FILE = "upgrade-drain-apply-block.txt";
    static final String APPLY_ENTERED_FILE = "upgrade-drain-apply-entered.txt";
    static final String APPLY_INTERRUPTED_FILE = "upgrade-drain-apply-interrupted.txt";
    static final String IGNORE_INTERRUPT_FILE = "upgrade-drain-ignore-interrupt.txt";
    static final String START_BLOCK_FILE = "upgrade-drain-start-block.txt";
    static final String START_ENTERED_FILE = "upgrade-drain-start-entered.txt";
    static final String HOOK_RESULT_FILE = "upgrade-drain-hook-result.txt";
    static final String HOOK_RELEASE_FILE = "upgrade-drain-hook-release.txt";
    static final String CONTEXT_AFTER_CANCEL_FILE = "upgrade-drain-context-after-cancel.txt";
    static final String ACTIVE_FLOW_FILE = "upgrade-drain-active-flow.txt";
    static final String HOOK_INVOCATIONS_FILE = "upgrade-drain-hook-invocations.txt";
    private static final List<String> SWITCH_FILES = List.of(
        APPLY_FAIL_FILE, APPLY_BLOCK_FILE, APPLY_ENTERED_FILE, APPLY_INTERRUPTED_FILE, IGNORE_INTERRUPT_FILE,
        START_BLOCK_FILE, START_ENTERED_FILE, HOOK_RESULT_FILE, HOOK_RELEASE_FILE, CONTEXT_AFTER_CANCEL_FILE,
        ACTIVE_FLOW_FILE, HOOK_INVOCATIONS_FILE);
    static final String RECORD_FILE = "upgrade-drain-evolution.txt";
    static final String PHASE_ONE_COMPLETE_RECORD_FILE = "upgrade-drain-phase-one-complete.txt";
    static final String PHASE_TWO_RECORD_FILE = "upgrade-drain-phase-two.txt";
    static final String REMOVED_CONNECTION = "Removed Connection";

    static final String DRAIN_CONNECTOR_TYPE = "org.apache.nifi.connectors.tests.system.UpgradeDrainConnector";
    static final String DRAIN_BUNDLE_GROUP = "org.apache.nifi";
    static final String DRAIN_BUNDLE_ARTIFACT = "nifi-connector-upgrade-drain-nar";
    static final String DRAIN_V1 = "1.0.0";
    static final String DRAIN_V2 = "2.0.0";
    static final String DRAIN_V3 = "3.0.0";
    static final String DRAIN_UNLOADABLE_VERSION = "4.0.0";
    private static final String DRAIN_NARS_LOCATION = "target/nifi-connector-upgrade-drain-nars";
    private static final Map<String, String> DRAIN_NAR_IDS = Map.of(
        DRAIN_V1, "nifi-connector-upgrade-drain-v1-nar",
        DRAIN_V2, "nifi-connector-upgrade-drain-v2-nar",
        DRAIN_V3, "nifi-connector-upgrade-drain-v3-nar",
        DRAIN_UNLOADABLE_VERSION, "nifi-connector-upgrade-drain-v4-nar");

    /**
     * Identifies a Connector bundle in assertions. A null version matches any version of the artifact.
     */
    protected record ExpectedBundle(String artifact, String version) {

        String recordPrefix() {
            return "org.apache.nifi:" + artifact + ":" + (version == null ? "" : version);
        }

        void assertMatches(final Bundle bundle, final String description) {
            assertNotNull(bundle, description);
            assertTrue(bundle.getArtifact().contains(artifact), description + " " + bundle.getArtifact());
            if (version != null) {
                assertEquals(version, bundle.getVersion(), description);
            }
        }
    }

    /**
     * Creates the Connector on its outgoing version and applies the given configuration.
     */
    protected abstract String createOutgoingConnector(Map<String, String> properties) throws Exception;

    /**
     * Moves the stopped Connector to its incoming version, leaving it STOPPED with the upgrade pending.
     */
    protected abstract void triggerUpgrade(String connectorId) throws Exception;

    protected abstract ExpectedBundle outgoingBundle();

    protected abstract ExpectedBundle incomingBundle();

    /**
     * Makes NiFi properties set by a test take effect before the Connector is created.
     */
    protected void applyInstanceProperties() throws Exception {
    }

    @BeforeEach
    public void clearUpgradeDrainSwitches() {
        for (final String switchFile : SWITCH_FILES) {
            deleteNodeFiles(switchFile);
        }
    }

    @Test
    @Timeout(360)
    public void testPendingUpgradeEditsThenStartsWithoutReEvolving() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, "original");
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        triggerUpgrade(connectorId);

        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertPending(connectorId);
        assertFalse(recordExists());

        getClientUtil().configureConnector(connectorId, "Upgrade", properties("DRAIN", false, true, "working-edit"));
        assertPending(connectorId);
        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
        assertNotNull(findProcessor(persisted.getManagedProcessGroup(), "TerminateFlowFile"));
        getNiFiInstance().start(true);
        waitForCluster();

        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        final String record = readRecord();
        assertTrue(record.startsWith("pre=" + outgoingBundle().recordPrefix()), record);
        assertTrue(record.contains("post=" + incomingBundle().recordPrefix()), record);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertEquals("working-edit", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());
        assertTrue(findProcessor(connectorId, "GenerateFlowFile").getComponent().getBundle().getArtifact().contains("nifi-system-test-extensions2"));

        deleteRecords();
        getNiFiInstance().stop();
        final VersionedConnector evolved = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertEquals(VersionedConnectorState.RUNNING, evolved.getScheduledState());
        assertEquals("working-edit", configurationProperty(evolved.getActiveFlowConfiguration(), "Comment"));
        assertEquals("working-edit", configurationProperty(evolved.getWorkingFlowConfiguration(), "Comment"));
        assertEquals("working-edit", findProcessor(evolved.getManagedProcessGroup(), "GenerateFlowFile").getComments());
        assertTrue(findProcessor(evolved.getManagedProcessGroup(), "GenerateFlowFile").getBundle().getArtifact().contains("nifi-system-test-extensions2"));
        getNiFiInstance().start(true);
        waitForCluster();
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertFalse(recordExists());
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertEquals("working-edit", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());

        getClientUtil().stopConnector(connectorId);
        getClientUtil().configureConnector(connectorId, "Upgrade", properties("DRAIN", false, true, "after-evolution"));
        getClientUtil().applyConnectorUpdate(getNifiClient().getConnectorClient().getConnector(connectorId));
        getClientUtil().waitForValidConnector(connectorId);
        assertEquals("after-evolution", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());
    }

    protected String createOutgoingConnector(final String mode, final boolean removeConnection, final String comment) throws Exception {
        return createOutgoingConnector(properties(mode, false, removeConnection, comment));
    }

    protected String queueStoppedUpgrade(final String mode, final boolean removeConnection, final String comment) throws Exception {
        return queueStopped(createOutgoingConnector(mode, removeConnection, comment));
    }

    protected String queueStopped(final String connectorId) throws Exception {
        deleteGates();
        getClientUtil().startConnector(connectorId);
        waitForConnectorMinQueueCount(connectorId, 1);
        getClientUtil().stopConnector(connectorId);
        return connectorId;
    }

    protected void restartKeepingIncomingVersion() {
        getNiFiInstance().stop();
        getNiFiInstance().start(true);
        waitForCluster();
    }

    protected void uploadDrainNars(final String... versions) throws Exception {
        final File narsLocation = new File(DRAIN_NARS_LOCATION);
        if (!narsLocation.exists()) {
            throw new IllegalStateException("NARs location does not exist at: " + narsLocation.getAbsolutePath());
        }

        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());
        for (final String version : versions) {
            final NarSummaryDTO nar = narUploadUtil.uploadNar(narsLocation, DRAIN_NAR_IDS.get(version));
            waitFor(narUploadUtil.getWaitForNarStateSupplier(nar.getIdentifier(), NarState.INSTALLED));
        }
    }

    protected String createDrainConnector(final String version, final Map<String, String> properties) throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector(DRAIN_CONNECTOR_TYPE, DRAIN_BUNDLE_GROUP, DRAIN_BUNDLE_ARTIFACT, version);
        getClientUtil().configureConnector(connector, "Upgrade", properties);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());
        return connector.getId();
    }

    /**
     * Sends the version change the way the UI does: a PUT carrying only the new bundle.
     */
    protected ConnectorEntity changeVersion(final String connectorId, final String version) throws NiFiClientException, IOException {
        return changeVersion(getNifiClient().getConnectorClient(), connectorId, version, false);
    }

    protected ConnectorEntity changeVersion(final ConnectorClient connectorClient, final String connectorId, final String version,
                                            final boolean disconnectedNodeAcknowledged) throws NiFiClientException, IOException {
        final ConnectorEntity current = connectorClient.getConnector(connectorId);
        final ConnectorDTO component = new ConnectorDTO();
        component.setId(connectorId);
        component.setBundle(new BundleDTO(DRAIN_BUNDLE_GROUP, DRAIN_BUNDLE_ARTIFACT, version));

        final ConnectorEntity update = new ConnectorEntity();
        update.setId(connectorId);
        update.setRevision(current.getRevision());
        update.setComponent(component);
        update.setDisconnectedNodeAcknowledged(disconnectedNodeAcknowledged);
        return connectorClient.updateConnector(update);
    }

    protected void createGates() throws IOException {
        writeNodeFile(GATE_FILE);
    }

    protected void createFailMarkers() throws IOException {
        writeNodeFile(FAIL_MARKER_FILE);
    }

    protected void deleteFailMarkers() {
        for (final File directory : nodeDirectories()) {
            final File marker = new File(directory, FAIL_MARKER_FILE);
            if (marker.exists()) {
                assertTrue(marker.delete());
            }
        }
    }

    private void writeNodeFile(final String fileName) throws IOException {
        writeNodeFiles(fileName, "");
    }

    protected void writeNodeFiles(final String fileName, final String contents) throws IOException {
        for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
            writeNodeFile(nodeIndex, fileName, contents);
        }
    }

    protected void writeNodeFile(final int nodeIndex, final String fileName, final String contents) throws IOException {
        Files.writeString(new File(nodeDirectory(nodeIndex), fileName).toPath(), contents);
    }

    protected void deleteNodeFiles(final String fileName) {
        for (final File directory : nodeDirectories()) {
            final File file = new File(directory, fileName);
            if (file.exists()) {
                assertTrue(file.delete(), file.getAbsolutePath());
            }
        }
    }

    protected void waitForNodeFile(final int nodeIndex, final String fileName) throws InterruptedException {
        final File file = new File(nodeDirectory(nodeIndex), fileName);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            if (file.isFile()) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("Timed out waiting for " + file.getAbsolutePath());
    }

    protected String readNodeFile(final int nodeIndex, final String fileName) throws IOException {
        return Files.readString(new File(nodeDirectory(nodeIndex), fileName).toPath());
    }

    protected void assertNotPending(final String connectorId) throws IOException {
        for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
            final VersionedConnector connector = FlowDefinitionFiles.readConnector(nodeInstance(nodeIndex), connectorId);
            assertManagedFlowBundle(connector, incomingBundle());
        }
    }

    protected void assertPersistedOutgoing(final String connectorId) throws IOException {
        for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
            final VersionedConnector connector = FlowDefinitionFiles.readConnector(nodeInstance(nodeIndex), connectorId);
            assertManagedFlowBundle(connector, outgoingBundle());
        }
    }

    protected void assertPersistedComment(final String connectorId, final String comment) throws IOException {
        for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
            final VersionedConnector connector = FlowDefinitionFiles.readConnector(nodeInstance(nodeIndex), connectorId);
            assertEquals(comment, findProcessor(connector.getManagedProcessGroup(), "GenerateFlowFile").getComments(), "node " + nodeIndex);
        }
    }

    protected int nodeCount() {
        return Math.max(1, getNumberOfNodes());
    }

    protected NiFiInstance nodeInstance(final int nodeIndex) {
        return getNumberOfNodes() <= 1 ? getNiFiInstance() : getNiFiInstance().getNodeInstance(nodeIndex);
    }

    protected File nodeDirectory(final int nodeIndex) {
        return nodeInstance(nodeIndex).getInstanceDirectory();
    }

    protected NiFiInstance firstNode() {
        return getNumberOfNodes() <= 1 ? getNiFiInstance() : getNiFiInstance().getNodeInstance(1);
    }

    protected List<File> nodeDirectories() {
        if (getNumberOfNodes() <= 1) {
            return List.of(getNiFiInstance().getInstanceDirectory());
        }
        final List<File> directories = new ArrayList<>();
        for (int nodeIndex = 1; nodeIndex <= getNumberOfNodes(); nodeIndex++) {
            directories.add(getNiFiInstance().getNodeInstance(nodeIndex).getInstanceDirectory());
        }
        return directories;
    }

    protected void requestStart(final String connectorId) throws NiFiClientException, IOException {
        final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
        entity.setDisconnectedNodeAcknowledged(true);
        getNifiClient().getConnectorClient().startConnector(entity);
    }

    protected static void assertLoadedBundle(final VersionedConnector connector, final String artifactFragment) {
        assertLoadedBundle(connector, new ExpectedBundle(artifactFragment, null));
    }

    protected static void assertLoadedBundle(final VersionedConnector connector, final ExpectedBundle expected) {
        expected.assertMatches(connector.getBundle(), "loaded bundle");
    }

    protected static void assertManagedFlowBundle(final VersionedConnector connector, final String artifactFragment) {
        assertManagedFlowBundle(connector, new ExpectedBundle(artifactFragment, null));
    }

    protected static void assertManagedFlowBundle(final VersionedConnector connector, final ExpectedBundle expected) {
        expected.assertMatches(connector.getManagedFlowBundle(), "managed flow bundle");
    }

    protected void assertPending(final String connectorId) {
        final Exception failure = assertThrows(Exception.class,
            () -> getClientUtil().applyConnectorUpdate(getNifiClient().getConnectorClient().getConnector(connectorId)));
        assertTrue(failure.getMessage().contains("start the connector instead"), failure.getMessage());
    }

    protected void waitUntilDraining(final String connectorId) throws NiFiClientException, IOException, InterruptedException {
        for (int iteration = 0; iteration < 600; iteration++) {
            final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
            final String state = entity.getComponent().getState();
            if (ConnectorState.DRAINING.name().equals(state)) {
                return;
            }
            if (ConnectorState.RUNNING.name().equals(state) || ConnectorState.STOPPED.name().equals(state) || ConnectorState.UPDATE_FAILED.name().equals(state)) {
                throw new AssertionError("Expected DRAINING but the connector is " + state + ": " + entity.getComponent().getValidationErrors());
            }
            Thread.sleep(100L);
        }
        final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
        throw new AssertionError("Connector did not reach DRAINING. State is " + entity.getComponent().getState());
    }

    protected String readRecord() throws IOException {
        final StringBuilder records = new StringBuilder();
        for (final File directory : nodeDirectories()) {
            final File record = new File(directory, RECORD_FILE);
            if (record.exists()) {
                records.append(Files.readString(record.toPath()));
            }
        }
        assertFalse(records.isEmpty(), "evolution record was not written");
        return records.toString();
    }

    protected String createConnector(final String type, final Map<String, String> properties) throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector(type);
        getClientUtil().configureConnector(connector, properties.containsKey("Evolution Mode") ? "Upgrade" : "Gate", properties);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());
        return connector.getId();
    }

    protected Map<String, String> properties(final String mode, final boolean ghost, final boolean removeConnection, final String comment) {
        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put("Gate File Path", GATE_FILE);
        properties.put("Evolution Mode", mode);
        properties.put("Ghost Components", Boolean.toString(ghost));
        properties.put("Remove Connection", Boolean.toString(removeConnection));
        if (comment != null) {
            properties.put("Comment", comment);
        }
        return properties;
    }

    protected void waitForRetryAfterQueueDrained(final String connectorId) throws Exception {
        waitFor(() -> {
            try {
                return getConnectorQueuedFlowFileCount(connectorId) == 0;
            } catch (final Exception e) {
                return false;
            }
        });

        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        String state = null;
        while (System.nanoTime() < deadline) {
            state = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getState();
            if (ConnectorState.RUNNING.name().equals(state)) {
                return;
            }
            if (ConnectorState.STOPPED.name().equals(state) || ConnectorState.UPDATE_FAILED.name().equals(state)) {
                break;
            }

            Thread.sleep(200L);
        }

        throw new AssertionError("Connector did not reach RUNNING after its queue drained; state=" + state
            + ", phaseOne=" + phaseRecordExists(PHASE_ONE_COMPLETE_RECORD_FILE)
            + ", phaseTwo=" + phaseRecordExists(PHASE_TWO_RECORD_FILE));
    }

    protected void deleteEvolutionPhaseRecords() {
        for (final File directory : nodeDirectories()) {
            deleteFile(directory, PHASE_ONE_COMPLETE_RECORD_FILE);
            deleteFile(directory, PHASE_TWO_RECORD_FILE);
        }
    }

    protected boolean phaseRecordExists(final String fileName) {
        return nodeDirectories().stream().anyMatch(directory -> new File(directory, fileName).exists());
    }

    private static void deleteFile(final File directory, final String fileName) {
        final File file = new File(directory, fileName);
        if (file.exists()) {
            assertTrue(file.delete());
        }
    }

    protected void deleteGates() {
        for (final File directory : nodeDirectories()) {
            final File gate = new File(directory, GATE_FILE);
            if (gate.exists()) {
                assertTrue(gate.delete());
            }
        }
    }

    protected void deleteRecords() {
        for (final File directory : nodeDirectories()) {
            final File record = new File(directory, RECORD_FILE);
            if (record.exists()) {
                assertTrue(record.delete());
            }
        }
    }

    protected boolean recordExists() {
        for (final File directory : nodeDirectories()) {
            if (new File(directory, RECORD_FILE).exists()) {
                return true;
            }
        }
        return false;
    }

    protected boolean observedDraining(final String connectorId) throws NiFiClientException, IOException {
        return ConnectorState.DRAINING.name().equals(getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getState());
    }

    protected void assertConnectorState(final String connectorId, final ConnectorState state) throws NiFiClientException, IOException {
        assertEquals(state.name(), getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getState());
    }

    protected String actionReason(final String connectorId, final String actionName) throws NiFiClientException, IOException {
        for (final ConnectorActionDTO action : getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getAvailableActions()) {
            if (actionName.equals(action.getName()) && !Boolean.TRUE.equals(action.getAllowed())) {
                return action.getReasonNotAllowed();
            }
        }
        return null;
    }

    protected ProcessorEntity findProcessor(final String connectorId, final String name) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flow = getNifiClient().getConnectorClient().getFlow(connectorId);
        for (final ProcessorEntity processor : flow.getProcessGroupFlow().getFlow().getProcessors()) {
            if (name.equals(processor.getComponent().getName())) {
                return processor;
            }
        }
        return null;
    }

    protected VersionedProcessor findProcessor(final VersionedProcessGroup group, final String name) {
        if (group == null || group.getProcessors() == null) {
            return null;
        }
        for (final VersionedProcessor processor : group.getProcessors()) {
            if (name.equals(processor.getName())) {
                return processor;
            }
        }
        return null;
    }

    protected String configurationProperty(final List<VersionedConfigurationStep> configurationSteps, final String propertyName) {
        for (final VersionedConfigurationStep configurationStep : configurationSteps) {
            if ("Upgrade".equals(configurationStep.getName()) && configurationStep.getProperties().containsKey(propertyName)) {
                return configurationStep.getProperties().get(propertyName).getValue();
            }
        }

        return null;
    }

    protected ConnectionEntity findConnection(final String connectorId, final String name) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flow = getNifiClient().getConnectorClient().getFlow(connectorId);
        if (flow.getProcessGroupFlow().getFlow().getConnections() == null) {
            return null;
        }
        for (final ConnectionEntity connection : flow.getProcessGroupFlow().getFlow().getConnections()) {
            if (name.equals(connection.getComponent().getName())) {
                return connection;
            }
        }
        return null;
    }

    protected void stopManagedProcessors(final String connectorId) throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupFlowEntity flow = getNifiClient().getConnectorClient().getFlow(connectorId);
        for (final ProcessorEntity processor : flow.getProcessGroupFlow().getFlow().getProcessors()) {
            getClientUtil().stopProcessor(processor);
        }
    }

    protected void waitForCluster() {
        if (getNumberOfNodes() > 1) {
            waitForAllNodesConnected();
        }
    }

    protected Map<File, Long> appLogSizes() {
        final Map<File, Long> sizes = new LinkedHashMap<>();
        for (final File directory : nodeDirectories()) {
            final File logFile = new File(new File(directory, "logs"), "nifi-app.log");
            sizes.put(logFile, logFile.exists() ? logFile.length() : 0L);
        }
        return sizes;
    }

    protected String appLogSince(final Map<File, Long> sizes) throws IOException {
        final StringBuilder appended = new StringBuilder();
        for (final Map.Entry<File, Long> entry : sizes.entrySet()) {
            final File logFile = entry.getKey();
            if (!logFile.exists()) {
                continue;
            }
            final byte[] content = Files.readAllBytes(logFile.toPath());
            final int offset = (int) Math.min(entry.getValue(), content.length);
            appended.append(new String(content, offset, content.length - offset, java.nio.charset.StandardCharsets.UTF_8));
        }
        return appended.toString();
    }

    protected Map<String, String> managedProcessorStates(final String connectorId) throws NiFiClientException, IOException {
        final Map<String, String> states = new LinkedHashMap<>();
        final ProcessGroupFlowEntity flow = getNifiClient().getConnectorClient().getFlow(connectorId);
        for (final ProcessorEntity processor : flow.getProcessGroupFlow().getFlow().getProcessors()) {
            states.put(processor.getComponent().getName(), processor.getComponent().getState());
        }
        return states;
    }

    protected static void assertAllStopped(final Map<String, String> processorStates) {
        assertFalse(processorStates.isEmpty(), "managed processors were not installed");
        processorStates.forEach((name, state) -> assertEquals("STOPPED", state, name));
    }
}
