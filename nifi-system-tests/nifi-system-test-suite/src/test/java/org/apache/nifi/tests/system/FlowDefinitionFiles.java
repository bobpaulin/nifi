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
package org.apache.nifi.tests.system;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.VersionedConnector;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Reads and rewrites connector sections of a stopped node's conf/flow.json.gz.
 */
public final class FlowDefinitionFiles {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private FlowDefinitionFiles() {
    }

    public static List<NiFiInstance> nodes(final NiFiInstance instance) {
        if (instance.getNumberOfNodes() <= 1) {
            return List.of(instance);
        }
        final List<NiFiInstance> nodes = new ArrayList<>();
        for (int nodeIndex = 1; nodeIndex <= instance.getNumberOfNodes(); nodeIndex++) {
            nodes.add(instance.getNodeInstance(nodeIndex));
        }
        return nodes;
    }

    public static VersionedConnector readConnector(final NiFiInstance node, final String connectorId) throws IOException {
        final VersionedDataflow dataflow = read(flowFile(node));
        return find(dataflow, connectorId);
    }

    public static void updateEveryNode(final NiFiInstance instance, final String connectorId, final Consumer<VersionedConnector> update) throws IOException {
        for (final NiFiInstance node : nodes(instance)) {
            final File flowFile = flowFile(node);
            final VersionedDataflow dataflow = read(flowFile);
            update.accept(find(dataflow, connectorId));
            write(flowFile, dataflow);
        }
    }

    public static void stripManagedProcessGroup(final NiFiInstance instance, final String connectorId) throws IOException {
        updateEveryNode(instance, connectorId, connector -> {
            connector.setManagedProcessGroup(null);
            connector.setManagedParameterContext(null);
            connector.setManagedFlowBundle(null);
        });
    }

    public static void rewriteManagedComponentBundleVersion(final NiFiInstance instance, final String connectorId, final String version) throws IOException {
        updateEveryNode(instance, connectorId, connector -> rewriteBundleVersion(connector.getManagedProcessGroup(), version));
    }

    private static void rewriteBundleVersion(final VersionedProcessGroup group, final String version) {
        if (group == null) {
            return;
        }
        if (group.getProcessors() != null) {
            for (final VersionedProcessor processor : group.getProcessors()) {
                if (processor.getBundle() != null) {
                    processor.getBundle().setVersion(version);
                }
            }
        }
        if (group.getControllerServices() != null) {
            for (final VersionedControllerService service : group.getControllerServices()) {
                if (service.getBundle() != null) {
                    service.getBundle().setVersion(version);
                }
            }
        }
        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
                rewriteBundleVersion(childGroup, version);
            }
        }
    }

    /**
     * Removes only {@code managedFlowBundle}, leaving the managed flow, parameters, active configuration, and the
     * bundle that identifies the code to load. This is a flow written before that field existed.
     */
    public static void stripManagedFlowBundle(final NiFiInstance instance, final String connectorId) throws IOException {
        updateEveryNode(instance, connectorId, connector -> connector.setManagedFlowBundle(null));
    }

    /**
     * @return the persisted value of the named parameter, from a connector snapshot or a flow parameter context
     */
    public static String parameterValue(final NiFiInstance instance, final String parameterName) throws IOException {
        for (final NiFiInstance node : nodes(instance)) {
            final VersionedDataflow dataflow = read(flowFile(node));
            if (dataflow.getConnectors() != null) {
                for (final VersionedConnector connector : dataflow.getConnectors()) {
                    final String value = parameterValue(connector.getManagedParameterContext(), parameterName);
                    if (value != null) {
                        return value;
                    }
                }
            }
            if (dataflow.getParameterContexts() != null) {
                for (final VersionedParameterContext context : dataflow.getParameterContexts()) {
                    final String value = parameterValue(context, parameterName);
                    if (value != null) {
                        return value;
                    }
                }
            }
        }
        return null;
    }

    private static String parameterValue(final VersionedParameterContext context, final String parameterName) {
        if (context == null || context.getParameters() == null) {
            return null;
        }
        for (final VersionedParameter parameter : context.getParameters()) {
            if (parameterName.equals(parameter.getName())) {
                return parameter.getValue();
            }
        }
        return null;
    }

    private static VersionedConnector find(final VersionedDataflow dataflow, final String connectorId) {
        if (dataflow.getConnectors() == null) {
            throw new IllegalStateException("flow.json.gz has no connectors");
        }
        for (final VersionedConnector connector : dataflow.getConnectors()) {
            if (connectorId.equals(connector.getInstanceIdentifier())) {
                return connector;
            }
        }
        throw new IllegalStateException("flow.json.gz has no connector " + connectorId);
    }

    private static File flowFile(final NiFiInstance node) {
        return new File(new File(node.getInstanceDirectory(), "conf"), "flow.json.gz");
    }

    private static VersionedDataflow read(final File flowFile) throws IOException {
        try (InputStream fileIn = new FileInputStream(flowFile);
             InputStream gzipIn = new GZIPInputStream(fileIn)) {
            return OBJECT_MAPPER.readValue(gzipIn, VersionedDataflow.class);
        }
    }

    private static void write(final File flowFile, final VersionedDataflow dataflow) throws IOException {
        try (OutputStream fileOut = new FileOutputStream(flowFile);
             OutputStream gzipOut = new GZIPOutputStream(fileOut)) {
            OBJECT_MAPPER.writeValue(gzipOut, dataflow);
        }
    }
}
