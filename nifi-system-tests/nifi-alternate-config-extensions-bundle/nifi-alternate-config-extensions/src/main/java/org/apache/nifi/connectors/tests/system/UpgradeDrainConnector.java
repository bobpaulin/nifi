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
import org.apache.nifi.components.connector.FlowEvolutionContext;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Post-upgrade connector. It drops the pre-upgrade queue connection unless configured to keep it, and records the evolution context.
 */
public class UpgradeDrainConnector extends AbstractConnector {

    public static final String RECORD_FILE_NAME = "upgrade-drain-evolution.txt";
    public static final String PHASE_ONE_COMPLETE_RECORD_FILE_NAME = "upgrade-drain-phase-one-complete.txt";
    public static final String PHASE_TWO_RECORD_FILE_NAME = "upgrade-drain-phase-two.txt";

    static final ConnectorPropertyDescriptor GATE_FILE_PATH = new ConnectorPropertyDescriptor.Builder()
        .name("Gate File Path")
        .description("Relative path of the file that lets the terminating processor run.")
        .required(true)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor EVOLUTION_MODE = new ConnectorPropertyDescriptor.Builder()
        .name("Evolution Mode")
        .description("How this connector handles connections removed by the upgrade.")
        .required(true)
        .defaultValue("DRAIN")
        .allowableValues("DRAIN", "INHERITED_DRAIN", "NO_OP", "FAIL")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor GHOST_COMPONENTS = new ConnectorPropertyDescriptor.Builder()
        .name("Ghost Components")
        .description("Accepted so a pre-upgrade configuration remains valid. This NAR does not contain those components.")
        .required(true)
        .defaultValue("false")
        .allowableValues("true", "false")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor REMOVE_CONNECTION = new ConnectorPropertyDescriptor.Builder()
        .name("Remove Connection")
        .description("When true, the upgraded flow drops the connection that queues generated FlowFiles.")
        .required(true)
        .defaultValue("true")
        .allowableValues("true", "false")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor COMMENT = new ConnectorPropertyDescriptor.Builder()
        .name("Comment")
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

    public static final String DRAIN_LABEL_NAME = "Drain Label";

    static final ConnectorPropertyDescriptor DRAIN_LABEL = new ConnectorPropertyDescriptor.Builder()
        .name(DRAIN_LABEL_NAME)
        .description("Label added by the upgraded connector.")
        .required(true)
        .defaultValue("upgraded")
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
        .addProperty(DRAIN_LABEL)
        .build();

    private static final ConfigurationStep CONFIG_STEP = new ConfigurationStep.Builder()
        .name("Upgrade")
        .propertyGroups(List.of(PROPERTY_GROUP))
        .build();

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
        final String activeFlowSwitch = readSwitch("upgrade-drain-active-flow.txt");
        if ("NULL".equals(activeFlowSwitch)) {
            return null;
        }
        if ("THROW".equals(activeFlowSwitch)) {
            throw new IllegalStateException("active flow failed");
        }
        return buildFlow(flowContext);
    }

    @Override
    public void start(final FlowContext activeContext) throws FlowUpdateException {
        if (Files.exists(Path.of("upgrade-drain-start-block.txt"))) {
            writeSwitch("upgrade-drain-start-entered.txt", "");
            awaitRemoved("upgrade-drain-start-block.txt", false);
        }
        super.start(activeContext);
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
        if (Files.exists(Path.of("upgrade-drain-apply-fail.txt"))) {
            throw new FlowUpdateException("apply failed");
        }
        if (Files.exists(Path.of("upgrade-drain-apply-block.txt"))) {
            writeSwitch("upgrade-drain-apply-entered.txt", "");
            final boolean ignoreInterrupt = Files.exists(Path.of("upgrade-drain-ignore-interrupt.txt"));
            try {
                awaitRemoved("upgrade-drain-apply-block.txt", ignoreInterrupt);
            } catch (final FlowUpdateException e) {
                writeSwitch("upgrade-drain-apply-interrupted.txt", "");
                throw e;
            }
        }

        try {
            Files.writeString(Path.of(PHASE_TWO_RECORD_FILE_NAME), "");
        } catch (final IOException e) {
            throw new FlowUpdateException("Failed to write the flow evolution Phase 2 record", e);
        }

        getInitializationContext().updateFlow(activeContext, buildFlow(workingContext), BundleCompatibility.RESOLVE_BUNDLE);
    }

    @Override
    public CompletableFuture<Void> prepareForFlowEvolution(final FlowEvolutionContext context) throws FlowUpdateException {
        appendSwitch("upgrade-drain-hook-invocations.txt", "invoked\n");
        final String hookResult = readSwitch("upgrade-drain-hook-result.txt");
        if ("NULL_FUTURE".equals(hookResult)) {
            return null;
        }
        if ("FAILED_FUTURE".equals(hookResult)) {
            return CompletableFuture.failedFuture(new FlowUpdateException("hook failed"));
        }
        if ("IGNORE_CANCEL".equals(hookResult)) {
            awaitPresent("upgrade-drain-hook-release.txt", true);
            writeSwitch("upgrade-drain-context-after-cancel.txt", contextState(context));
            return CompletableFuture.completedFuture(null);
        }

        writeRecord(context);
        final String mode = property(context.getPreUpgradeActiveFlowContext(), EVOLUTION_MODE);
        // The hook reads the persisted active configuration, so FAIL stays set across a retry.
        // upgrade-drain-fail.txt is the transient condition a retry clears; without it, FAIL drains.
        if ("FAIL".equals(mode) && Files.exists(Path.of("upgrade-drain-fail.txt"))) {
            throw new FlowUpdateException("evolution failed");
        }
        if ("INHERITED_DRAIN".equals(mode)) {
            final CompletableFuture<Void> drainFuture = drainFlowFiles(context.getPreUpgradeActiveFlowContext());
            drainFuture.thenRun(UpgradeDrainConnector::writePhaseOneRecordUnchecked);
            return drainFuture;
        } else if ("DRAIN".equals(mode) || "FAIL".equals(mode)) {
            // Start only the consumer. Starting the generator as well refills the queue faster than it can empty.
            final ProcessGroupFacade rootGroup = context.getPreUpgradeActiveFlowContext().getRootGroup();
            try {
                for (final ProcessorFacade processor : rootGroup.getProcessors()) {
                    if ("TerminateFlowFile".equals(processor.getDefinition().getName())) {
                        processor.getLifecycle().start().get();
                    }
                }
            } catch (final Exception e) {
                throw new FlowUpdateException("Failed to start the pre-upgrade flow so its queues can drain", e);
            }

            while (rootGroup.getConnections().stream().anyMatch(connection -> connection.getQueueSize().getObjectCount() > 0)) {
                try {
                    Thread.sleep(200L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new FlowUpdateException("Flow evolution was interrupted", e);
                }
            }
        }

        writePhaseOneRecord();
        return CompletableFuture.completedFuture(null);
    }

    private static void writePhaseOneRecord() throws FlowUpdateException {
        try {
            Files.writeString(Path.of(PHASE_ONE_COMPLETE_RECORD_FILE_NAME), "");
        } catch (final IOException e) {
            throw new FlowUpdateException("Failed to write the flow evolution Phase 1 completion record", e);
        }
    }

    private static void writePhaseOneRecordUnchecked() {
        try {
            writePhaseOneRecord();
        } catch (final FlowUpdateException e) {
            throw new CompletionException(e);
        }
    }

    private static void writeRecord(final FlowEvolutionContext context) throws FlowUpdateException {
        final Bundle pre = context.getPreUpgradeActiveFlowContext().getBundle();
        final Bundle post = context.getPostUpgradeBundle();
        final String record = "pre=" + bundle(pre) + "\npost=" + bundle(post) + "\n";
        try {
            Files.writeString(Path.of(RECORD_FILE_NAME), record);
        } catch (final IOException e) {
            throw new FlowUpdateException("Failed to write the evolution record", e);
        }
    }

    private static String bundle(final Bundle bundle) {
        return bundle.getGroup() + ":" + bundle.getArtifact() + ":" + bundle.getVersion();
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

        final boolean removeConnection = flowContext == null || Boolean.parseBoolean(property(flowContext, REMOVE_CONNECTION));
        if (removeConnection) {
            generate.setAutoTerminatedRelationships(Set.of("success"));
        } else {
            final VersionedProcessor terminate = VersionedFlowUtils.addProcessor(rootGroup, TERMINATE_TYPE, GATE_BUNDLE, "TerminateFlowFile", new Position(400, 0));
            if (gateFile != null) {
                terminate.getProperties().put("Gate File", gateFile);
            }
            final VersionedConnection connection = VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(generate),
                VersionedFlowUtils.createConnectableComponent(terminate), Set.of("success"));
            connection.setName("Removed Connection");
        }

        if (Boolean.parseBoolean(property(flowContext, NESTED_GROUP))) {
            addNestedGroup(rootGroup, gateFile);
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

    private static void awaitRemoved(final String fileName, final boolean ignoreInterrupt) throws FlowUpdateException {
        await(Path.of(fileName), true, ignoreInterrupt);
    }

    private static void awaitPresent(final String fileName, final boolean ignoreInterrupt) throws FlowUpdateException {
        await(Path.of(fileName), false, ignoreInterrupt);
    }

    private static void await(final Path path, final boolean untilRemoved, final boolean ignoreInterrupt) throws FlowUpdateException {
        while (Files.exists(path) == untilRemoved) {
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException e) {
                if (ignoreInterrupt) {
                    continue;
                }
                Thread.currentThread().interrupt();
                throw new FlowUpdateException("Flow evolution was interrupted", e);
            }
        }
    }

    private static String readSwitch(final String fileName) {
        try {
            return Files.exists(Path.of(fileName)) ? Files.readString(Path.of(fileName)).trim() : null;
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to read " + fileName, e);
        }
    }

    private static void writeSwitch(final String fileName, final String contents) throws FlowUpdateException {
        try {
            Files.writeString(Path.of(fileName), contents);
        } catch (final IOException e) {
            throw new FlowUpdateException("Failed to write " + fileName, e);
        }
    }

    private static void appendSwitch(final String fileName, final String contents) throws FlowUpdateException {
        try {
            Files.writeString(Path.of(fileName), contents, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (final IOException e) {
            throw new FlowUpdateException("Failed to append " + fileName, e);
        }
    }

    private static String contextState(final FlowEvolutionContext context) {
        try {
            context.getPreUpgradeActiveFlowContext().getRootGroup();
            return "OPEN";
        } catch (final RuntimeException e) {
            return "CLOSED";
        }
    }
}
