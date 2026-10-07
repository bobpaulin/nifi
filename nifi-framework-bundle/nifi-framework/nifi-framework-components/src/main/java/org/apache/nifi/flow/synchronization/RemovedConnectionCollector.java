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

package org.apache.nifi.flow.synchronization;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.mapping.VersionedComponentFlowMapper;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Finds connections in a live process group that a proposed versioned flow removes.
 * The rules match {@code StandardVersionedComponentSynchronizer#removeMissingConnections} and the child-group
 * removal performed by {@code StandardVersionedComponentSynchronizer#verifyCanRemoveMissingComponents}: connections
 * are matched by versioned component id within each process group, a connection whose source versioned id changed
 * counts as removed, and a child group absent from the proposed flow removes every connection in it and its descendants.
 */
public final class RemovedConnectionCollector {

    private RemovedConnectionCollector() {
    }

    public static Set<Connection> findConnectionsRemovedBy(final ProcessGroup current, final VersionedProcessGroup proposed) {
        final Set<Connection> removed = new HashSet<>();
        collect(current, proposed, removed);
        return removed;
    }

    private static void collect(final ProcessGroup current, final VersionedProcessGroup proposed, final Set<Connection> removed) {
        final Map<String, Connection> connectionsByVersionedId = current.getConnections().stream()
            .collect(Collectors.toMap(RemovedConnectionCollector::versionedId, Function.identity(), (left, right) -> left));

        final Set<String> proposedConnectionIds = versionedConnections(proposed).stream()
            .map(VersionedConnection::getIdentifier)
            .collect(Collectors.toSet());

        for (final Map.Entry<String, Connection> entry : connectionsByVersionedId.entrySet()) {
            if (!proposedConnectionIds.contains(entry.getKey())) {
                removed.add(entry.getValue());
            }
        }

        for (final VersionedConnection proposedConnection : versionedConnections(proposed)) {
            final Connection existingConnection = connectionsByVersionedId.get(proposedConnection.getIdentifier());
            if (existingConnection == null || proposedConnection.getSource() == null || existingConnection.getSource() == null) {
                continue;
            }
            final String existingSourceId = existingConnection.getSource().getVersionedComponentId().orElse(null);
            final String proposedSourceId = proposedConnection.getSource().getId();
            if (existingSourceId != null && !existingSourceId.equals(proposedSourceId)) {
                removed.add(existingConnection);
            }
        }

        final Map<String, VersionedProcessGroup> proposedChildren = versionedGroups(proposed).stream()
            .collect(Collectors.toMap(VersionedProcessGroup::getIdentifier, Function.identity(), (left, right) -> left));

        for (final ProcessGroup child : current.getProcessGroups()) {
            final String childVersionedId = child.getVersionedComponentId()
                .orElseGet(() -> VersionedComponentFlowMapper.generateVersionedComponentId(child.getIdentifier()));
            final VersionedProcessGroup proposedChild = proposedChildren.get(childVersionedId);
            if (proposedChild == null) {
                removed.addAll(child.findAllConnections());
            } else {
                collect(child, proposedChild, removed);
            }
        }
    }

    private static String versionedId(final Connection connection) {
        return connection.getVersionedComponentId()
            .orElseGet(() -> VersionedComponentFlowMapper.generateVersionedComponentId(connection.getIdentifier()));
    }

    private static Collection<VersionedConnection> versionedConnections(final VersionedProcessGroup group) {
        return group.getConnections() == null ? Collections.emptySet() : group.getConnections();
    }

    private static Collection<VersionedProcessGroup> versionedGroups(final VersionedProcessGroup group) {
        return group.getProcessGroups() == null ? Collections.emptySet() : group.getProcessGroups();
    }
}
