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

import org.apache.nifi.components.connector.FlowEvolutionStatus;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Upgrade-drain scenarios that run with a flow evolution timeout short enough to expire while FlowFiles are held.
 */
public class ConnectorFlowEvolutionTimeoutIT extends AbstractChangeVersionDrainIT {

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of(NiFiProperties.CONNECTOR_FLOW_EVOLUTION_TIMEOUT, "10 secs");
    }

    @Test
    @Timeout(240)
    public void testUpgradeDrainExceedsTimeout() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        changeVersion(connectorId, DRAIN_V2);
        requestStart(connectorId);
        waitUntilDraining(connectorId);

        assertStoppedPending(connectorId, "timed out");
        assertFlowEvolutionStatus(connectorId, FlowEvolutionStatus.FAILED);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertPersistedOutgoing(connectorId);
    }
}
