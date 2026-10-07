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

package org.apache.nifi.cluster.coordination.http.replication;

import jakarta.ws.rs.core.Response;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.NoConnectedNodesException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.connector.FlowEvolutionStatus;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClusteredConnectorRequestReplicatorTest {

    private static final String CONNECTOR_ID = "9eb6002f-a705-4c99-9e1c-4e0d93465a7e";

    @Test
    void testMembershipMustSettleBeforeParticipantsAreEstablished() throws Exception {
        final RequestReplicator requestReplicator = mock(RequestReplicator.class);
        final NodeIdentifier firstNode = mock(NodeIdentifier.class);
        final NodeIdentifier secondNode = mock(NodeIdentifier.class);
        final AsyncClusterResponse response = response(Set.of(firstNode, secondNode), FlowEvolutionStatus.PHASE_ONE_PENDING);
        when(requestReplicator.replicate(isNull(), eq("GET"), any(), anyMap(), anyMap())).thenReturn(response);
        when(requestReplicator.isMembershipSettled()).thenReturn(false, true, true, false);

        final ClusteredConnectorRequestReplicator replicator = new ClusteredConnectorRequestReplicator(() -> requestReplicator, false);

        assertThrows(NoConnectedNodesException.class, () -> replicator.getFlowEvolutionStatus(CONNECTOR_ID));
        assertEquals(FlowEvolutionStatus.PHASE_ONE_PENDING, replicator.getFlowEvolutionStatus(CONNECTOR_ID));
        assertThrows(IOException.class, () -> replicator.getFlowEvolutionStatus(CONNECTOR_ID));
    }

    @Test
    void testParticipantLossFailsEstablishedBarrier() throws Exception {
        final RequestReplicator requestReplicator = mock(RequestReplicator.class);
        final NodeIdentifier firstNode = mock(NodeIdentifier.class);
        final NodeIdentifier secondNode = mock(NodeIdentifier.class);
        final AsyncClusterResponse completeMembership = response(Set.of(firstNode, secondNode), FlowEvolutionStatus.PHASE_ONE_PENDING);
        final AsyncClusterResponse reducedMembership = response(Set.of(firstNode), FlowEvolutionStatus.PHASE_ONE_COMPLETE);
        when(requestReplicator.replicate(isNull(), eq("GET"), any(), anyMap(), anyMap())).thenReturn(completeMembership, reducedMembership);
        when(requestReplicator.isMembershipSettled()).thenReturn(true);

        final ClusteredConnectorRequestReplicator replicator = new ClusteredConnectorRequestReplicator(() -> requestReplicator, false);

        assertEquals(FlowEvolutionStatus.PHASE_ONE_PENDING, replicator.getFlowEvolutionStatus(CONNECTOR_ID));
        assertThrows(IOException.class, () -> replicator.getFlowEvolutionStatus(CONNECTOR_ID));
    }

    @Test
    void testTimedStatusReadUsesProvidedDeadline() throws Exception {
        final RequestReplicator requestReplicator = mock(RequestReplicator.class);
        final NodeIdentifier node = mock(NodeIdentifier.class);
        final AsyncClusterResponse response = response(Set.of(node), FlowEvolutionStatus.PHASE_ONE_COMPLETE);
        when(requestReplicator.replicate(isNull(), eq("GET"), any(), anyMap(), anyMap())).thenReturn(response);
        when(requestReplicator.isMembershipSettled()).thenReturn(true);
        final Duration timeout = Duration.ofMillis(250);

        final ClusteredConnectorRequestReplicator replicator = new ClusteredConnectorRequestReplicator(() -> requestReplicator, false);

        assertEquals(FlowEvolutionStatus.PHASE_ONE_COMPLETE, replicator.getFlowEvolutionStatus(CONNECTOR_ID, timeout));
        verify(response).awaitMergedResponse(longThat(wait -> wait > 0 && wait <= timeout.toNanos()), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void testIncompleteParticipantDoesNotAdvanceBarrier() throws Exception {
        final RequestReplicator requestReplicator = mock(RequestReplicator.class);
        final NodeIdentifier firstNode = mock(NodeIdentifier.class);
        final NodeIdentifier secondNode = mock(NodeIdentifier.class);
        final AsyncClusterResponse response = response(Set.of(firstNode, secondNode), FlowEvolutionStatus.PHASE_ONE_COMPLETE);
        final NodeResponse failedNode = mock(NodeResponse.class);
        when(failedNode.is2xx()).thenReturn(false);
        when(response.getNodeResponse(secondNode)).thenReturn(failedNode);
        when(requestReplicator.replicate(isNull(), eq("GET"), any(), anyMap(), anyMap())).thenReturn(response);
        when(requestReplicator.isMembershipSettled()).thenReturn(true);

        final ClusteredConnectorRequestReplicator replicator = new ClusteredConnectorRequestReplicator(() -> requestReplicator, false);

        assertEquals(FlowEvolutionStatus.INACTIVE, replicator.getFlowEvolutionStatus(CONNECTOR_ID));
    }

    @Test
    void testTimedStatusReadFailsWhenMergedResponseTimesOut() throws Exception {
        final RequestReplicator requestReplicator = mock(RequestReplicator.class);
        final AsyncClusterResponse response = mock(AsyncClusterResponse.class);
        when(response.awaitMergedResponse(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(null);
        when(requestReplicator.replicate(isNull(), eq("GET"), any(), anyMap(), anyMap())).thenReturn(response);
        when(requestReplicator.isMembershipSettled()).thenReturn(true);

        final ClusteredConnectorRequestReplicator replicator = new ClusteredConnectorRequestReplicator(() -> requestReplicator, false);

        assertThrows(IOException.class, () -> replicator.getFlowEvolutionStatus(CONNECTOR_ID, Duration.ofMillis(10)));
    }

    private AsyncClusterResponse response(final Set<NodeIdentifier> participants, final FlowEvolutionStatus status) throws InterruptedException {
        final ConnectorDTO connector = new ConnectorDTO();
        connector.setId(CONNECTOR_ID);
        connector.setFlowEvolutionStatus(status.name());
        final ConnectorEntity connectorEntity = new ConnectorEntity();
        connectorEntity.setComponent(connector);

        final Response clientResponse = Response.ok().build();
        final NodeResponse nodeResponse = mock(NodeResponse.class);
        when(nodeResponse.getUpdatedEntity()).thenReturn(connectorEntity);
        when(nodeResponse.getClientResponse()).thenReturn(clientResponse);
        when(nodeResponse.is2xx()).thenReturn(true);

        final AsyncClusterResponse asyncResponse = mock(AsyncClusterResponse.class);
        when(asyncResponse.awaitMergedResponse()).thenReturn(nodeResponse);
        when(asyncResponse.awaitMergedResponse(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(nodeResponse);
        when(asyncResponse.getNodesInvolved()).thenReturn(participants);
        when(asyncResponse.getNodeResponse(any())).thenReturn(nodeResponse);
        return asyncResponse;
    }
}
