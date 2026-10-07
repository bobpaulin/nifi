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
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Upgrade-drain scenarios whose trigger is a NAR swap and restart. A running Connector resumes the upgrade during
 * startup, before every node has joined, and a restart while draining must come back pending.
 */
public abstract class AbstractNarSwapUpgradeDrainIT extends AbstractConnectorUpgradeDrainIT {

    static final String V1_BUNDLE = "nifi-system-test-extensions-nar";
    static final String V2_BUNDLE = "nifi-alternate-config-extensions-nar";

    @Override
    protected String createOutgoingConnector(final Map<String, String> properties) throws Exception {
        return createConnector("UpgradeDrainConnector", properties);
    }

    @Override
    protected void triggerUpgrade(final String connectorId) throws IOException {
        swapAndRestart();
    }

    @Override
    protected ExpectedBundle outgoingBundle() {
        return new ExpectedBundle(V1_BUNDLE, null);
    }

    @Override
    protected ExpectedBundle incomingBundle() {
        return new ExpectedBundle(V2_BUNDLE, null);
    }

    @Test
    @Timeout(240)
    public void testRunningConnectorAutoResumes() throws Exception {
        final String connectorId = createUpgradeConnector("DRAIN", false, true, null);
        deleteGates();
        getClientUtil().startConnector(connectorId);
        waitForConnectorMinQueueCount(connectorId, 1);
        createGates();
        getNiFiInstance().stop();
        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();

        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertTrue(recordExists());
    }

    @Test
    @Timeout(240)
    public void testRestartDuringDrainingComesBackPending() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        triggerUpgrade(connectorId);
        deleteGates();
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) > 0);
        requestStart(connectorId);
        waitUntilDraining(connectorId);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        restartKeepingIncomingVersion();
        getClientUtil().cancelDrain(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertPending(connectorId);
        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
    }

    protected String createUpgradeConnector(final String mode, final boolean ghost, final boolean removeConnection, final String comment)
            throws NiFiClientException, IOException, InterruptedException {
        return createConnector("UpgradeDrainConnector", properties(mode, ghost, removeConnection, comment));
    }

    protected String queueStoppedUpgrade(final String mode, final boolean ghost, final boolean removeConnection, final String comment) throws Exception {
        return queueStopped(createUpgradeConnector(mode, ghost, removeConnection, comment));
    }

    protected void swapAndRestart() throws IOException {
        getNiFiInstance().stop();
        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();
    }
}
