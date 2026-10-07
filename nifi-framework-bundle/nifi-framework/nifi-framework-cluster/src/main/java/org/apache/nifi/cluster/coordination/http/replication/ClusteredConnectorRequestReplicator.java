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
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.Response.Status.Family;
import jakarta.ws.rs.core.Response.StatusType;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.NoConnectedNodesException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.components.connector.FlowEvolutionRequestReplicator;
import org.apache.nifi.components.connector.FlowEvolutionStatus;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ClusteredConnectorRequestReplicator implements FlowEvolutionRequestReplicator {
    private static final Logger logger = LoggerFactory.getLogger(ClusteredConnectorRequestReplicator.class);
    private static final String GET = "GET";

    private static final Duration STATUS_POLL_SLICE = Duration.ofSeconds(1);

    private final Supplier<RequestReplicator> requestReplicatorSupplier;
    private final String replicationScheme;
    private final ConcurrentMap<String, Set<NodeIdentifier>> flowEvolutionParticipants = new ConcurrentHashMap<>();

    public ClusteredConnectorRequestReplicator(final Supplier<RequestReplicator> requestReplicatorSupplier, final boolean httpsEnabled) {
        this.requestReplicatorSupplier = Objects.requireNonNull(requestReplicatorSupplier, "Request Replicator Supplier required");
        this.replicationScheme = httpsEnabled ? "https" : "http";
    }

    @Override
    public ConnectorState getState(final String connectorId) throws IOException {
        final RequestReplicator requestReplicator = getRequestReplicator();
        if (!requestReplicator.isMembershipSettled()) {
            throw new NoConnectedNodesException();
        }

        final ConnectorClusterResponse connectorResponse = getConnectorResponse(requestReplicator, connectorId);
        final String stateName = connectorResponse.connectorEntity().getComponent().getState();
        logger.debug("getState: Connector [{}] — state from response: [{}]", connectorId, stateName);
        try {
            return ConnectorState.valueOf(stateName);
        } catch (final IllegalArgumentException e) {
            throw new IOException("Received unknown Connector state '" + stateName + "' for Connector with ID " + connectorId);
        }
    }

    @Override
    public FlowEvolutionStatus getFlowEvolutionStatus(final String connectorId) throws IOException {
        return getFlowEvolutionStatus(connectorId, null);
    }

    @Override
    public FlowEvolutionStatus getFlowEvolutionStatus(final String connectorId, final Duration timeout) throws IOException {
        final long deadlineNanos = System.nanoTime() + (timeout == null ? Duration.ofMinutes(30) : timeout).toNanos();
        while (true) {
            final long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new IOException("Timed out waiting for clustered flow evolution status for Connector with ID " + connectorId);
            }

            final Duration slice = Duration.ofNanos(Math.min(remainingNanos, STATUS_POLL_SLICE.toNanos()));
            try {
                return readFlowEvolutionStatus(connectorId, slice);
            } catch (final StatusPollTimeoutException e) {
                if (System.nanoTime() >= deadlineNanos) {
                    throw new IOException("Timed out waiting for clustered flow evolution status for Connector with ID " + connectorId, e);
                }
            }
        }
    }

    private FlowEvolutionStatus readFlowEvolutionStatus(final String connectorId, final Duration timeout) throws IOException {
        final RequestReplicator requestReplicator = getRequestReplicator();
        final Set<NodeIdentifier> expectedParticipants = flowEvolutionParticipants.get(connectorId);
        if (!requestReplicator.isMembershipSettled()) {
            if (expectedParticipants == null) {
                throw new NoConnectedNodesException();
            }

            throw new IOException("Cluster membership changed while waiting for Connector flow evolution " + connectorId);
        }

        final ConnectorClusterResponse connectorResponse = getConnectorResponse(requestReplicator, connectorId, timeout);
        if (!requestReplicator.isMembershipSettled()) {
            if (expectedParticipants == null) {
                throw new NoConnectedNodesException();
            }

            throw new IOException("Cluster membership changed while waiting for Connector flow evolution " + connectorId);
        }

        final Set<NodeIdentifier> currentParticipants = connectorResponse.nodesInvolved();
        final Set<NodeIdentifier> establishedParticipants;
        if (expectedParticipants == null) {
            final Set<NodeIdentifier> participantSnapshot = Set.copyOf(currentParticipants);
            final Set<NodeIdentifier> concurrentParticipants = flowEvolutionParticipants.putIfAbsent(connectorId, participantSnapshot);
            establishedParticipants = concurrentParticipants == null ? participantSnapshot : concurrentParticipants;
        } else {
            establishedParticipants = expectedParticipants;
        }

        if (!establishedParticipants.equals(currentParticipants)) {
            throw new IOException("Cluster membership changed while waiting for Connector flow evolution " + connectorId
                    + "; expected participants " + establishedParticipants + " but found " + currentParticipants);
        }

        // The merged entity omits a node whose response failed. Advancing on that entity lets the remaining nodes
        // leave the barrier while a participant is still in an earlier phase.
        if (!connectorResponse.everyNodeSucceeded()) {
            logger.warn("Connector [{}] flow evolution status is incomplete for participants {}", connectorId, currentParticipants);
            return FlowEvolutionStatus.INACTIVE;
        }

        final String statusName = connectorResponse.connectorEntity().getComponent().getFlowEvolutionStatus();
        if (statusName == null) {
            throw new IOException("Received no flow evolution status for Connector with ID " + connectorId);
        }

        try {
            return FlowEvolutionStatus.valueOf(statusName);
        } catch (final IllegalArgumentException e) {
            throw new IOException("Received unknown flow evolution status '" + statusName + "' for Connector with ID " + connectorId, e);
        }
    }

    @Override
    public void resetFlowEvolutionParticipants(final String connectorId) {
        flowEvolutionParticipants.remove(connectorId);
    }

    private ConnectorClusterResponse getConnectorResponse(final RequestReplicator requestReplicator, final String connectorId) throws IOException {
        return getConnectorResponse(requestReplicator, connectorId, null);
    }

    private ConnectorClusterResponse getConnectorResponse(
            final RequestReplicator requestReplicator,
            final String connectorId,
            final Duration timeout) throws IOException {
        final URI uri = URI.create(replicationScheme + "://localhost/nifi-api/connectors/" + connectorId);

        logger.debug("Connector [{}] — replicating GET to URI [{}]", connectorId, uri);

        // User is null. GET /connectors/{id} API endpoint has special logic for authorizing requests made by nifi cluster nodes based on mTLS cert SANs
        final AsyncClusterResponse asyncResponse = requestReplicator.replicate(null, GET, uri, Map.of(), Map.of());

        try {
            final NodeResponse mergedNodeResponse = timeout == null
                    ? asyncResponse.awaitMergedResponse()
                    : asyncResponse.awaitMergedResponse(timeout.toNanos(), TimeUnit.NANOSECONDS);
            if (mergedNodeResponse == null) {
                throw new StatusPollTimeoutException("Timed out waiting for clustered flow evolution status for Connector with ID " + connectorId);
            }

            final Entity updatedEntity = mergedNodeResponse.getUpdatedEntity();
            final Response response = mergedNodeResponse.getClientResponse();
            if (response != null) {
                final int statusCode = response.getStatusInfo().getStatusCode();
                logger.debug("Connector [{}] — merged response status [{}] ({})", connectorId, statusCode, response.getStatusInfo().getReasonPhrase());
                verifyResponse(response.getStatusInfo(), connectorId);
            } else if (mergedNodeResponse.hasThrowable()) {
                throw new IOException("Failed to retrieve Connector state for " + connectorId, mergedNodeResponse.getThrowable());
            } else if (!(updatedEntity instanceof ConnectorEntity)) {
                throw new IOException("Received neither response nor merged Connector entity while retrieving state for Connector " + connectorId);
            }

            // Use the merged/updated entity if available, otherwise fall back to reading from the raw response.
            // The updatedEntity contains the properly merged state from all nodes, while readEntity() would
            // only return the state from whichever single node was selected as the "client response".
            final ConnectorEntity connectorEntity;
            if (updatedEntity instanceof final ConnectorEntity mergedConnectorEntity) {
                logger.debug("Connector [{}] - using merged updatedEntity", connectorId);
                connectorEntity = mergedConnectorEntity;
            } else {
                logger.debug("Connector [{}] — updatedEntity is [{}], falling back to readEntity()", connectorId,
                        updatedEntity == null ? "null" : updatedEntity.getClass().getSimpleName());
                connectorEntity = response.readEntity(ConnectorEntity.class);
            }

            if (connectorEntity == null) {
                throw new IOException("Received null ConnectorEntity for Connector with ID " + connectorId);
            }
            if (connectorEntity.getComponent() == null) {
                throw new IOException("Received ConnectorEntity with null component for Connector with ID " + connectorId
                        + ". Permissions canRead=" + (connectorEntity.getPermissions() != null ? connectorEntity.getPermissions().getCanRead() : "null"));
            }

            final Set<NodeIdentifier> nodesInvolved = Set.copyOf(asyncResponse.getNodesInvolved());
            boolean everyNodeSucceeded = true;
            for (final NodeIdentifier node : nodesInvolved) {
                final NodeResponse nodeResponse = asyncResponse.getNodeResponse(node);
                if (nodeResponse == null || !nodeResponse.is2xx()) {
                    everyNodeSucceeded = false;
                    break;
                }
            }

            return new ConnectorClusterResponse(connectorEntity, nodesInvolved, everyNodeSucceeded);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for response for status of Connector with ID " + connectorId, e);
        }
    }

    @Override
    public void setFlowManager(final FlowManager flowManager) {
    }

    private RequestReplicator getRequestReplicator() {
        final RequestReplicator requestReplicator = requestReplicatorSupplier.get();
        return Objects.requireNonNull(requestReplicator, "Request Replicator required");
    }

    private void verifyResponse(final StatusType responseStatusType, final String connectorId) throws IOException {
        final int statusCode = responseStatusType.getStatusCode();
        final String reason = responseStatusType.getReasonPhrase();

        if (responseStatusType.getStatusCode() == Status.NOT_FOUND.getStatusCode()) {
            throw new IllegalArgumentException("Connector with ID " + connectorId + " does not exist");
        }

        final Family responseFamily = responseStatusType.getFamily();
        if (responseFamily == Family.SERVER_ERROR) {
            throw new IOException("Server-side error requesting State for Connector with ID " + connectorId + ". Status code: " + statusCode + ", reason: " + reason);
        }
        if (responseFamily == Family.CLIENT_ERROR) {
            throw new IOException("Client-side error requesting State for Connector with ID " + connectorId + ". Status code: " + statusCode + ", reason: " + reason);
        }
        if (responseFamily != Family.SUCCESSFUL) {
            throw new IOException("Unexpected response code " + statusCode + " returned when requesting State for Connector with ID " + connectorId + ": " + reason);
        }
    }

    private record ConnectorClusterResponse(ConnectorEntity connectorEntity, Set<NodeIdentifier> nodesInvolved, boolean everyNodeSucceeded) {
    }

    private static final class StatusPollTimeoutException extends IOException {
        private StatusPollTimeoutException(final String message) {
            super(message);
        }
    }
}
