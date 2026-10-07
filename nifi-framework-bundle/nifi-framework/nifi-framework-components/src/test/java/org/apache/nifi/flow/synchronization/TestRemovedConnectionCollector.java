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
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.mapping.VersionedComponentFlowMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRemovedConnectionCollector {

    @Test
    public void testConnectionRemovedFromTopLevelGroup() {
        final Connection connection = connection("conn-1", "instance-1", "source-1");
        final ProcessGroup group = group(Set.of(connection), Set.of());
        final VersionedProcessGroup proposed = new VersionedProcessGroup();

        final Set<Connection> removed = RemovedConnectionCollector.findConnectionsRemovedBy(group, proposed);

        assertEquals(Set.of(connection), removed);
    }

    @Test
    public void testConnectionRemovedFromNestedGroup() {
        final Connection connection = connection("nested-conn", "nested-instance", "source-1");
        final ProcessGroup child = group(Set.of(connection), Set.of());
        when(child.getVersionedComponentId()).thenReturn(Optional.of("child-1"));
        final ProcessGroup root = group(Set.of(), Set.of(child));

        final VersionedProcessGroup proposedChild = new VersionedProcessGroup();
        proposedChild.setIdentifier("child-1");
        final VersionedProcessGroup proposed = new VersionedProcessGroup();
        proposed.setProcessGroups(Set.of(proposedChild));

        assertEquals(Set.of(connection), RemovedConnectionCollector.findConnectionsRemovedBy(root, proposed));
    }

    @Test
    public void testRemovedChildGroupRemovesDescendantConnections() {
        final Connection connection = connection("inner-conn", "inner-instance", "source-1");
        final ProcessGroup child = group(Set.of(), Set.of());
        when(child.getVersionedComponentId()).thenReturn(Optional.of("child-1"));
        when(child.findAllConnections()).thenReturn(List.of(connection));
        final ProcessGroup root = group(Set.of(), Set.of(child));

        assertEquals(Set.of(connection), RemovedConnectionCollector.findConnectionsRemovedBy(root, new VersionedProcessGroup()));
    }

    @Test
    public void testSourceChangeCountsAsRemoved() {
        final Connection connection = connection("conn-1", "instance-1", "source-old");
        final ProcessGroup group = group(Set.of(connection), Set.of());
        final VersionedProcessGroup proposed = proposedWithConnection("conn-1", "source-new", "dest-1");

        assertEquals(Set.of(connection), RemovedConnectionCollector.findConnectionsRemovedBy(group, proposed));
    }

    @Test
    public void testDestinationChangeDoesNotCountAsRemoved() {
        final Connection connection = connection("conn-1", "instance-1", "source-1");
        final ProcessGroup group = group(Set.of(connection), Set.of());
        final VersionedProcessGroup proposed = proposedWithConnection("conn-1", "source-1", "dest-new");

        assertTrue(RemovedConnectionCollector.findConnectionsRemovedBy(group, proposed).isEmpty());
    }

    @Test
    public void testGeneratedVersionedIdFallback() {
        final Connection connection = mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("raw-id");
        when(connection.getVersionedComponentId()).thenReturn(Optional.empty());
        final ProcessGroup group = group(Set.of(connection), Set.of());

        final String generatedId = VersionedComponentFlowMapper.generateVersionedComponentId("raw-id");
        final VersionedProcessGroup matching = proposedWithConnection(generatedId, "source-1", "dest-1");
        final org.apache.nifi.connectable.Connectable source = connectable("source-1");
        when(connection.getSource()).thenReturn(source);
        assertTrue(RemovedConnectionCollector.findConnectionsRemovedBy(group, matching).isEmpty());

        assertEquals(Set.of(connection), RemovedConnectionCollector.findConnectionsRemovedBy(group, new VersionedProcessGroup()));
    }

    @Test
    public void testUnchangedFlowReturnsEmptySet() {
        final Connection connection = connection("conn-1", "instance-1", "source-1");
        final ProcessGroup group = group(Set.of(connection), Set.of());
        final VersionedProcessGroup proposed = proposedWithConnection("conn-1", "source-1", "dest-1");

        assertTrue(RemovedConnectionCollector.findConnectionsRemovedBy(group, proposed).isEmpty());
    }

    private static ProcessGroup group(final Set<Connection> connections, final Set<ProcessGroup> children) {
        final ProcessGroup group = mock(ProcessGroup.class);
        when(group.getConnections()).thenReturn(connections);
        when(group.getProcessGroups()).thenReturn(children);
        when(group.getVersionedComponentId()).thenReturn(Optional.empty());
        return group;
    }

    private static Connection connection(final String versionedId, final String instanceId, final String sourceVersionedId) {
        final Connection connection = mock(Connection.class);
        final org.apache.nifi.connectable.Connectable source = connectable(sourceVersionedId);
        when(connection.getIdentifier()).thenReturn(instanceId);
        when(connection.getVersionedComponentId()).thenReturn(Optional.of(versionedId));
        when(connection.getSource()).thenReturn(source);
        return connection;
    }

    private static org.apache.nifi.connectable.Connectable connectable(final String versionedId) {
        final org.apache.nifi.connectable.Connectable connectable = mock(org.apache.nifi.connectable.Connectable.class);
        when(connectable.getVersionedComponentId()).thenReturn(Optional.of(versionedId));
        return connectable;
    }

    private static VersionedProcessGroup proposedWithConnection(final String connectionId, final String sourceId, final String destinationId) {
        final VersionedConnection connection = new VersionedConnection();
        connection.setIdentifier(connectionId);
        connection.setSource(component(sourceId));
        connection.setDestination(component(destinationId));
        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setConnections(Set.of(connection));
        return group;
    }

    private static ConnectableComponent component(final String id) {
        final ConnectableComponent component = new ConnectableComponent();
        component.setId(id);
        return component;
    }
}
