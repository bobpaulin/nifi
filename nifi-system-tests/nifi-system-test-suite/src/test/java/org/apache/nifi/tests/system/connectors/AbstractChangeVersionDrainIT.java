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
import org.apache.nifi.tests.system.FlowDefinitionFiles;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Upgrade-drain scenarios whose trigger is a version change while the Connector is stopped. The rollback tests run
 * here so a two-node cluster still sees a failure on one node roll the other node back.
 */
public abstract class AbstractChangeVersionDrainIT extends AbstractConnectorUpgradeDrainIT {

    @BeforeEach
    public void uploadDrainVersions() throws Exception {
        uploadDrainNars(DRAIN_V1, DRAIN_V2, DRAIN_V3);
    }

    @Override
    protected String createOutgoingConnector(final Map<String, String> properties) throws Exception {
        return createDrainConnector(DRAIN_V1, properties);
    }

    @Override
    protected void triggerUpgrade(final String connectorId) throws Exception {
        changeVersion(connectorId, DRAIN_V2);
    }

    @Override
    protected ExpectedBundle outgoingBundle() {
        return new ExpectedBundle(DRAIN_BUNDLE_ARTIFACT, DRAIN_V1);
    }

    @Override
    protected ExpectedBundle incomingBundle() {
        return new ExpectedBundle(DRAIN_BUNDLE_ARTIFACT, DRAIN_V2);
    }

    @Override
    protected void applyInstanceProperties() {
        restartKeepingIncomingVersion();
    }

    @Test
    @Timeout(240)
    public void testDrainTimeoutThenRetry() throws Exception {
        getNiFiInstance().setProperty(NiFiProperties.CONNECTOR_FLOW_EVOLUTION_TIMEOUT, "5 secs");
        applyInstanceProperties();
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        triggerUpgrade(connectorId);
        deleteGates();
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) > 0);

        requestStart(connectorId);
        waitUntilDraining(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertPending(connectorId);

        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
    }

    @Test
    @Timeout(240)
    public void testTroubleshootingRejectedWhileDraining() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        triggerUpgrade(connectorId);
        deleteGates();
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) > 0);

        requestStart(connectorId);
        waitUntilDraining(connectorId);
        final String reason = actionReason(connectorId, "ENTER_TROUBLESHOOTING");
        assertNotNull(reason);
        assertTrue(reason.contains("currently"));
        assertThrows(NiFiClientException.class, () -> getClientUtil().enterTroubleshooting(connectorId));
        assertConnectorState(connectorId, ConnectorState.DRAINING);
        final Exception rejectedEdit = assertThrows(Exception.class,
            () -> getClientUtil().configureConnector(connectorId, "Upgrade", properties("DRAIN", false, true, "during-drain")));
        assertTrue(rejectedEdit.getMessage().contains("draining an upgrade"), rejectedEdit.getMessage());
        assertConnectorState(connectorId, ConnectorState.DRAINING);

        getClientUtil().cancelDrain(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertPending(connectorId);
        deleteRecords();

        getClientUtil().enterTroubleshooting(connectorId);
        final NiFiClientException rejected = assertThrows(NiFiClientException.class, () -> getClientUtil().endTroubleshooting(connectorId));
        assertTrue(rejected.getMessage().contains("upgrade is pending"), rejected.getMessage());
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));

        getClientUtil().emptyQueue(connectionId);
        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertFalse(recordExists());
        getNiFiInstance().stop();
        final VersionedConnector exited = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(exited, incomingBundle());
        assertManagedFlowBundle(exited, incomingBundle());
    }

    @Test
    @Timeout(240)
    public void testApplyFailureOnFirstNodeRollsBackEveryNode() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, "original");
        changeVersion(connectorId, DRAIN_V2);
        getClientUtil().configureConnector(connectorId, "Upgrade", properties("NO_OP", false, false, "edited"));
        writeNodeFile(1, APPLY_FAIL_FILE, "");

        requestStart(connectorId);
        assertStoppedPending(connectorId, "apply failed");
        assertEquals("original", processorComment(connectorId));
        assertPersistedComment(connectorId, "original");
        assertPersistedOutgoing(connectorId);

        deleteNodeFiles(APPLY_FAIL_FILE);
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertEquals("edited", processorComment(connectorId));
        assertNotPending(connectorId);
    }

    @Test
    @Timeout(240)
    public void testCancelDuringBlockedApplyRollsBack() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, "original");
        changeVersion(connectorId, DRAIN_V2);
        getClientUtil().configureConnector(connectorId, "Upgrade", properties("NO_OP", false, false, "edited"));
        writeNodeFiles(APPLY_BLOCK_FILE, "");
        try {
            requestStart(connectorId);
            waitForEveryNodeFile(APPLY_ENTERED_FILE);
            assertConnectorState(connectorId, ConnectorState.DRAINING);
            getClientUtil().cancelDrain(connectorId);
            waitForEveryNodeFile(APPLY_INTERRUPTED_FILE);
            deleteNodeFiles(APPLY_BLOCK_FILE);
            assertStoppedPending(connectorId, null);
            assertEquals("original", processorComment(connectorId));
            assertPersistedComment(connectorId, "original");
            assertPersistedOutgoing(connectorId);
        } finally {
            releaseBlockedApply();
        }
    }

    @Test
    @Timeout(300)
    public void testBlockedApplyPastTimeoutRollsBack() throws Exception {
        final String connectorId = queueBlockedApply(false);
        try {
            waitForEveryNodeFile(APPLY_ENTERED_FILE);
            getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);
            assertPending(connectorId);
            assertEveryNodeFileExists(APPLY_INTERRUPTED_FILE);
            assertEquals("original", processorComment(connectorId));

            deleteNodeFiles(APPLY_BLOCK_FILE);
            assertPersistedComment(connectorId, "original");
            assertPersistedOutgoing(connectorId);
        } finally {
            releaseBlockedApply();
        }
    }

    @Test
    @Timeout(300)
    public void testNonCooperativeApplyPastTimeoutRollsBack() throws Exception {
        final String connectorId = queueBlockedApply(true);
        try {
            waitForEveryNodeFile(APPLY_ENTERED_FILE);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            String state = null;
            while (System.nanoTime() < deadline) {
                state = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getState();
                assertFalse(ConnectorState.RUNNING.name().equals(state), state);
                if (ConnectorState.STOPPED.name().equals(state) || ConnectorState.UPDATE_FAILED.name().equals(state)) {
                    break;
                }
                Thread.sleep(200L);
            }
            assertFalse(ConnectorState.RUNNING.name().equals(state), state);
            assertPending(connectorId);

            deleteNodeFiles(APPLY_BLOCK_FILE);
            deleteNodeFiles(IGNORE_INTERRUPT_FILE);
            getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);
            assertPending(connectorId);
            assertEquals("original", processorComment(connectorId));
            assertPersistedComment(connectorId, "original");
            assertPersistedOutgoing(connectorId);
        } finally {
            releaseBlockedApply();
        }
    }

    @Test
    @Timeout(300)
    public void testBlockedStartAfterCommitStaysStopped() throws Exception {
        getNiFiInstance().setProperty(NiFiProperties.CONNECTOR_FLOW_EVOLUTION_TIMEOUT, "10 secs");
        applyInstanceProperties();
        final String connectorId = queueStoppedUpgrade("DRAIN", true, "original");
        changeVersion(connectorId, DRAIN_V2);
        writeNodeFiles(START_BLOCK_FILE, "");
        createGates();
        try {
            requestStart(connectorId);
            waitForEveryNodeFile(START_ENTERED_FILE);
            // start() returns only after the block file is removed. The timeout moves the connector to STOPPING
            // and leaves it there until start() gets past the block.
            final long stoppingDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            String state = null;
            while (System.nanoTime() < stoppingDeadline) {
                state = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getState();
                if (ConnectorState.STOPPING.name().equals(state)) {
                    break;
                }
                Thread.sleep(200L);
            }
            assertEquals(ConnectorState.STOPPING.name(), state, "connector did not reach STOPPING after the start timeout");
            deleteNodeFiles(START_BLOCK_FILE);
            getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);

            assertConnectorState(connectorId, ConnectorState.STOPPED);
            assertNull(findConnection(connectorId, REMOVED_CONNECTION));
            final long persistedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < persistedDeadline) {
                try {
                    assertNotPending(connectorId);
                    if (FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId).getScheduledState() == VersionedConnectorState.ENABLED) {
                        break;
                    }
                } catch (final FileNotFoundException ignored) {
                    // The stopped-state save replaces flow.json.gz.
                }
                Thread.sleep(200L);
            }
            for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
                final VersionedConnector persisted = FlowDefinitionFiles.readConnector(nodeInstance(nodeIndex), connectorId);
                assertEquals(VersionedConnectorState.ENABLED, persisted.getScheduledState(), "node " + nodeIndex);
            }
        } finally {
            releaseBlockedApply();
        }
    }

    @Test
    @Timeout(240)
    public void testHookIgnoringCancellationKeepsDrainingUntilItReturns() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        changeVersion(connectorId, DRAIN_V2);
        writeNodeFiles(HOOK_RESULT_FILE, "IGNORE_CANCEL");
        final ExecutorService cancelExecutor = Executors.newSingleThreadExecutor();
        try {
            requestStart(connectorId);
            waitUntilDraining(connectorId);
            // Cancellation returns while the hook is still inside the connector, so the release file has to be written
            // on this thread. The hook does not return until that file exists.
            final Future<?> cancel = cancelExecutor.submit(() -> getClientUtil().cancelDrain(connectorId));
            requestStart(connectorId);
            Thread.sleep(3000L);
            // The hook ignores cancellation, so the connector leaves DRAINING for UPDATE_FAILED and stays there
            // until the hook returns. STOPPED would mean the upgrade had already rolled back.
            assertConnectorState(connectorId, ConnectorState.UPDATE_FAILED);
            assertSingleHookInvocation();

            writeNodeFiles(HOOK_RELEASE_FILE, "");
            cancel.get(60, TimeUnit.SECONDS);
            assertStoppedPending(connectorId, null);
            assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
            assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
            for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
                assertEquals("CLOSED", readNodeFile(nodeIndex, CONTEXT_AFTER_CANCEL_FILE).trim(), "node " + nodeIndex);
            }
            assertPersistedOutgoing(connectorId);
        } finally {
            writeNodeFiles(HOOK_RELEASE_FILE, "");
            cancelExecutor.shutdownNow();
        }
    }

    @Test
    @Timeout(240)
    public void testSecondStartWhileDrainingJoinsEvolution() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        changeVersion(connectorId, DRAIN_V2);
        deleteGates();

        requestStart(connectorId);
        waitUntilDraining(connectorId);
        requestStart(connectorId);
        createGates();
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertSingleHookInvocation();
        assertNotPending(connectorId);
    }

    @Test
    @Timeout(240)
    public void testHookContractViolationsStayPending() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, null);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        changeVersion(connectorId, DRAIN_V2);

        final Map<String, String> violations = new LinkedHashMap<>();
        violations.put("NULL_FUTURE", "must return a CompletableFuture");
        violations.put("FAILED_FUTURE", "hook failed");
        violations.put("NULL", "Upgraded flow was not returned");
        violations.put("THROW", "active flow failed");
        for (final Map.Entry<String, String> violation : violations.entrySet()) {
            deleteNodeFiles(HOOK_RESULT_FILE);
            deleteNodeFiles(ACTIVE_FLOW_FILE);
            if ("NULL".equals(violation.getKey()) || "THROW".equals(violation.getKey())) {
                writeNodeFiles(ACTIVE_FLOW_FILE, violation.getKey());
            } else {
                writeNodeFiles(HOOK_RESULT_FILE, violation.getKey());
            }

            requestStart(connectorId);
            assertStoppedPending(connectorId, violation.getValue());
            assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
            assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
            assertPersistedOutgoing(connectorId);
        }
    }

    private String queueBlockedApply(final boolean ignoreInterrupt) throws Exception {
        getNiFiInstance().setProperty(NiFiProperties.CONNECTOR_FLOW_EVOLUTION_TIMEOUT, "10 secs");
        applyInstanceProperties();
        final String connectorId = queueStoppedUpgrade("NO_OP", false, "original");
        changeVersion(connectorId, DRAIN_V2);
        getClientUtil().configureConnector(connectorId, "Upgrade", properties("NO_OP", false, false, "edited"));
        writeNodeFiles(APPLY_BLOCK_FILE, "");
        if (ignoreInterrupt) {
            writeNodeFiles(IGNORE_INTERRUPT_FILE, "");
        }
        requestStart(connectorId);
        return connectorId;
    }

    private void releaseBlockedApply() throws IOException {
        deleteNodeFiles(APPLY_BLOCK_FILE);
        deleteNodeFiles(IGNORE_INTERRUPT_FILE);
        deleteNodeFiles(START_BLOCK_FILE);
    }

    private void assertStoppedPending(final String connectorId, final String validationError) throws Exception {
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);
        assertPending(connectorId);
        if (validationError == null) {
            return;
        }
        // A failure on one node is reported there as the connector's message. The other node reports that some node failed.
        final StringBuilder seen = new StringBuilder();
        for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
            final String errors = validationErrors(nodeIndex, connectorId);
            seen.append("node ").append(nodeIndex).append('=').append(errors).append('\n');
            if (errors.contains(validationError)) {
                return;
            }
        }
        throw new AssertionError(seen.toString());
    }

    private String validationErrors(final int nodeIndex, final String connectorId) throws NiFiClientException, IOException {
        if (nodeCount() == 1) {
            return getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getValidationErrors().toString();
        }
        switchClientToNode(nodeIndex);
        try {
            return getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connectorId).getComponent().getValidationErrors().toString();
        } finally {
            setupClient();
        }
    }

    private String processorComment(final String connectorId) throws NiFiClientException, IOException {
        return findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments();
    }

    private void waitForEveryNodeFile(final String fileName) throws InterruptedException {
        for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
            waitForNodeFile(nodeIndex, fileName);
        }
    }

    private void assertEveryNodeFileExists(final String fileName) {
        for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
            assertTrue(new File(nodeDirectory(nodeIndex), fileName).isFile(), fileName + " on node " + nodeIndex);
        }
    }

    private void assertSingleHookInvocation() throws IOException {
        for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
            final String[] lines = readNodeFile(nodeIndex, HOOK_INVOCATIONS_FILE).trim().split("\\R");
            assertEquals(1, lines.length, "node " + nodeIndex + " invocations: " + String.join(",", lines));
            assertEquals("invoked", lines[0]);
        }
    }
}
