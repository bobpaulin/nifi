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
package org.apache.nifi.components.connector;

import org.apache.nifi.controller.flow.FlowManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandaloneConnectorRequestReplicator {

    @Test
    public void testGetFlowEvolutionStatusReturnsLocalStatus() {
        final String connectorId = "connector-1";
        final ConnectorNode connectorNode = mock(ConnectorNode.class);
        when(connectorNode.getFlowEvolutionStatus()).thenReturn(FlowEvolutionStatus.PHASE_TWO_COMPLETE);
        final FlowManager flowManager = mock(FlowManager.class);
        when(flowManager.getConnector(connectorId)).thenReturn(connectorNode);
        final StandaloneConnectorRequestReplicator replicator = new StandaloneConnectorRequestReplicator();
        replicator.setFlowManager(flowManager);

        assertEquals(FlowEvolutionStatus.PHASE_TWO_COMPLETE, replicator.getFlowEvolutionStatus(connectorId));
    }

    @Test
    public void testGetFlowEvolutionStatusRejectsMissingConnector() {
        final StandaloneConnectorRequestReplicator replicator = new StandaloneConnectorRequestReplicator();
        replicator.setFlowManager(mock(FlowManager.class));

        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> replicator.getFlowEvolutionStatus("missing"));

        assertEquals("No connector found with id: missing", failure.getMessage());
    }
}
