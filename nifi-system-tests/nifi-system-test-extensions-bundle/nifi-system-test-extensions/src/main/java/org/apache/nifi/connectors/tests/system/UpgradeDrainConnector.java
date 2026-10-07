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
package org.apache.nifi.connectors.tests.system;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.BundleCompatibility;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pre-upgrade connector. Its flow queues FlowFiles on a connection that the alternate-config copy of this class removes.
 */
public class UpgradeDrainConnector extends AbstractConnector {

    public static final String GATE_FILE_PATH_NAME = "Gate File Path";
    public static final String EVOLUTION_MODE_NAME = "Evolution Mode";
    public static final String GHOST_COMPONENTS_NAME = "Ghost Components";
    public static final String REMOVE_CONNECTION_NAME = "Remove Connection";
    public static final String COMMENT_NAME = "Comment";
    public static final String REMOVED_CONNECTION_NAME = "Removed Connection";

    static final ConnectorPropertyDescriptor GATE_FILE_PATH = new ConnectorPropertyDescriptor.Builder()
        .name(GATE_FILE_PATH_NAME)
        .description("Relative path of the file that lets the terminating processor run.")
        .required(true)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor EVOLUTION_MODE = new ConnectorPropertyDescriptor.Builder()
        .name(EVOLUTION_MODE_NAME)
        .description("How the upgraded connector handles removed connections.")
        .required(true)
        .defaultValue("DRAIN")
        .allowableValues("DRAIN", "INHERITED_DRAIN", "NO_OP", "FAIL")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor GHOST_COMPONENTS = new ConnectorPropertyDescriptor.Builder()
        .name(GHOST_COMPONENTS_NAME)
        .description("When true, the pre-upgrade flow includes a processor and service that exist only in this NAR.")
        .required(true)
        .defaultValue("false")
        .allowableValues("true", "false")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor REMOVE_CONNECTION = new ConnectorPropertyDescriptor.Builder()
        .name(REMOVE_CONNECTION_NAME)
        .description("When true, the upgraded flow drops the connection that queues generated FlowFiles.")
        .required(true)
        .defaultValue("true")
        .allowableValues("true", "false")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor COMMENT = new ConnectorPropertyDescriptor.Builder()
        .name(COMMENT_NAME)
        .description("Comment copied onto the generating processor.")
        .required(false)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final String NESTED_GROUP_NAME = "Nested Group";

    static final ConnectorPropertyDescriptor NESTED_GROUP = new ConnectorPropertyDescriptor.Builder()
        .name(NESTED_GROUP_NAME)
        .description("When true, the flow includes a child group whose queue must be drained through a nested processor.")
        .required(true)
        .defaultValue("false")
        .allowableValues("true", "false")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    public static final String LEGACY_NOTE_NAME = "Legacy Note";

    static final ConnectorPropertyDescriptor LEGACY_NOTE = new ConnectorPropertyDescriptor.Builder()
        .name(LEGACY_NOTE_NAME)
        .description("Optional note retained only by this version.")
        .required(false)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Upgrade")
        .addProperty(GATE_FILE_PATH)
        .addProperty(EVOLUTION_MODE)
        .addProperty(GHOST_COMPONENTS)
        .addProperty(REMOVE_CONNECTION)
        .addProperty(COMMENT)
        .addProperty(NESTED_GROUP)
        .addProperty(LEGACY_NOTE)
        .build();

    private static final ConfigurationStep CONFIG_STEP = new ConfigurationStep.Builder()
        .name("Upgrade")
        .propertyGroups(List.of(PROPERTY_GROUP))
        .build();

    static final Bundle EXTENSIONS_BUNDLE = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "2.13.0-SNAPSHOT");
    private static final Bundle GATE_BUNDLE = new Bundle("org.apache.nifi", "nifi-system-test-extensions2-nar", "2.13.0-SNAPSHOT");
    private static final String ROOT_GROUP_ID = "upgrade-drain-root";
    private static final String NESTED_GROUP_ID = "upgrade-drain-nested";
    private static final String GENERATE_TYPE = "org.apache.nifi.processors.tests.system.UpgradeDrainGenerate";
    private static final String TERMINATE_TYPE = "org.apache.nifi.processors.tests.system.UpgradeDrainTerminate";

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return buildFlow(null);
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext flowContext) {
        return buildFlow(flowContext);
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIG_STEP);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        getInitializationContext().updateFlow(activeContext, buildFlow(workingContext), BundleCompatibility.RESOLVE_BUNDLE);
    }

    private VersionedExternalFlow buildFlow(final FlowContext flowContext) {
        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup(ROOT_GROUP_ID, "Upgrade Drain");
        final VersionedProcessor generate = VersionedFlowUtils.addProcessor(rootGroup, GENERATE_TYPE, GATE_BUNDLE, "GenerateFlowFile", new Position(0, 0));
        generate.getProperties().put("File Size", "1 B");
        generate.setSchedulingPeriod("100 millis");
        final String gateFile = property(flowContext, GATE_FILE_PATH);
        if (gateFile != null) {
            generate.getProperties().put("Gate File", gateFile);
        }

        final String comment = property(flowContext, COMMENT);
        if (comment != null && !comment.isBlank()) {
            generate.setComments(comment);
        }

        final VersionedProcessor terminate = VersionedFlowUtils.addProcessor(rootGroup, TERMINATE_TYPE, GATE_BUNDLE, "TerminateFlowFile", new Position(400, 0));
        if (gateFile != null) {
            terminate.getProperties().put("Gate File", gateFile);
        }

        final VersionedConnection connection = VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(generate),
            VersionedFlowUtils.createConnectableComponent(terminate), Set.of("success"));
        connection.setName(REMOVED_CONNECTION_NAME);

        if (Boolean.parseBoolean(property(flowContext, NESTED_GROUP))) {
            addNestedGroup(rootGroup, gateFile);
        }

        if (Boolean.parseBoolean(property(flowContext, GHOST_COMPONENTS))) {
            final VersionedProcessor ghostProcessor = VersionedFlowUtils.addProcessor(rootGroup,
                "org.apache.nifi.processors.tests.system.UpgradeOnlyV1Processor", EXTENSIONS_BUNDLE, "UpgradeOnlyV1Processor", new Position(0, 200));
            ghostProcessor.setAutoTerminatedRelationships(Set.of("success"));
            VersionedFlowUtils.addControllerService(rootGroup, "org.apache.nifi.cs.tests.system.UpgradeOnlyV1Service", EXTENSIONS_BUNDLE, "UpgradeOnlyV1Service");
        }

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        return flow;
    }

    private static void addNestedGroup(final VersionedProcessGroup rootGroup, final String gateFile) {
        final VersionedProcessGroup nested = VersionedFlowUtils.createProcessGroup(NESTED_GROUP_ID, "Nested Drain");
        nested.setPosition(new Position(0, 400));
        nested.setGroupIdentifier(rootGroup.getIdentifier());
        rootGroup.getProcessGroups().add(nested);

        final VersionedProcessor nestedGenerate = VersionedFlowUtils.addProcessor(nested, GENERATE_TYPE, GATE_BUNDLE, "Nested Generate", new Position(0, 0));
        nestedGenerate.getProperties().put("File Size", "1 B");
        nestedGenerate.setSchedulingPeriod("100 millis");
        if (gateFile != null) {
            nestedGenerate.getProperties().put("Gate File", gateFile);
        }

        final VersionedProcessor nestedTerminate = VersionedFlowUtils.addProcessor(nested, TERMINATE_TYPE, GATE_BUNDLE, "Nested Terminate", new Position(400, 0));
        if (gateFile != null) {
            nestedTerminate.getProperties().put("Gate File", gateFile);
        }

        final VersionedConnection nestedQueue = VersionedFlowUtils.addConnection(nested, VersionedFlowUtils.createConnectableComponent(nestedGenerate),
            VersionedFlowUtils.createConnectableComponent(nestedTerminate), Set.of("success"));
        nestedQueue.setName("Nested Queue");
    }

    private static String property(final FlowContext flowContext, final ConnectorPropertyDescriptor descriptor) {
        if (flowContext == null) {
            return descriptor.getDefaultValue();
        }
        final String value = flowContext.getConfigurationContext().getProperty(CONFIG_STEP, descriptor).getValue();
        return value == null ? descriptor.getDefaultValue() : value;
    }
}
