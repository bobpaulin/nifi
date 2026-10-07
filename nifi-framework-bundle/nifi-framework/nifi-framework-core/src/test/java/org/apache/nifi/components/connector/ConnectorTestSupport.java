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

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessGroupLifecycle;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.MockStateManagerProvider;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.VersionedConnector;
import org.apache.nifi.flow.VersionedConnectorState;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.FlowFileConcurrency;
import org.apache.nifi.groups.FlowFileOutboundPolicy;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;
import org.apache.nifi.security.encryption.PropertyEncryptionProviderInitializationContext;
import org.apache.nifi.security.encryption.SensitivePropertyContext;
import org.apache.nifi.util.MockComponentLog;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class ConnectorTestSupport {

    private ConnectorTestSupport() {
    }

    static VersionedProcessGroup flowGroup(final String name) {
        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setName(name);
        group.setIdentifier(name);
        return group;
    }

    static VersionedConnector versioned(final Bundle bundle, final VersionedProcessGroup managed, final VersionedConnectorState state) {
        final VersionedConnector versioned = new VersionedConnector();
        versioned.setInstanceIdentifier("connector-1");
        versioned.setName("Connector");
        versioned.setType(RecordingConnector.class.getName());
        versioned.setScheduledState(state);
        versioned.setBundle(bundle);
        versioned.setManagedProcessGroup(managed);
        versioned.setActiveFlowConfiguration(List.of());
        versioned.setWorkingFlowConfiguration(List.of());
        return versioned;
    }

    static Harness harness(final RecordingConnector connector, final boolean extensionMissing) throws Exception {
        return harness(connector, extensionMissing, true);
    }

    static Harness harness(final RecordingConnector connector, final boolean extensionMissing,
            final boolean explicitFlowEvolutionBarrier) throws Exception {
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getIdentifier()).thenReturn("managed-group");
        when(processGroup.getProcessGroupIdentifier()).thenReturn("managed-group");
        when(processGroup.getName()).thenReturn("Managed Group");
        when(processGroup.getPosition()).thenReturn(new Position(0, 0));
        when(processGroup.getFlowFileConcurrency()).thenReturn(FlowFileConcurrency.UNBOUNDED);
        when(processGroup.getFlowFileOutboundPolicy()).thenReturn(FlowFileOutboundPolicy.STREAM_WHEN_AVAILABLE);
        when(processGroup.getExecutionEngine()).thenReturn(ExecutionEngine.INHERITED);
        when(processGroup.purge()).thenReturn(CompletableFuture.completedFuture(null));
        when(processGroup.getQueueSize()).thenReturn(new QueueSize(0, 0L));
        when(processGroup.getProcessors()).thenReturn(Set.of());
        when(processGroup.getProcessGroups()).thenReturn(Set.of());
        when(processGroup.getInputPorts()).thenReturn(Set.of());
        when(processGroup.getOutputPorts()).thenReturn(Set.of());
        when(processGroup.getRemoteProcessGroups()).thenReturn(Set.of());
        when(processGroup.getControllerServices(false)).thenReturn(Set.of());
        when(processGroup.findAllProcessors()).thenReturn(List.of());
        when(processGroup.findAllControllerServices()).thenReturn(Set.of());
        when(processGroup.getConnections()).thenReturn(Set.of());

        final ProcessGroupLifecycle lifecycle = mock(ProcessGroupLifecycle.class);
        when(lifecycle.stop()).thenReturn(CompletableFuture.completedFuture(null));
        final ProcessGroupFacade rootFacade = mock(ProcessGroupFacade.class);
        when(rootFacade.getConnections()).thenReturn(Set.of());
        when(rootFacade.getProcessGroups()).thenReturn(Set.of());
        when(rootFacade.getLifecycle()).thenReturn(lifecycle);

        final AssetManager assetManager = mock(AssetManager.class);
        when(assetManager.getAssets(any())).thenReturn(List.of());
        final SecretsManager secretsManager = mock(SecretsManager.class);
        when(secretsManager.getAllSecrets()).thenReturn(List.of());
        when(secretsManager.getSecrets(any())).thenReturn(Map.of());

        final ProcessGroupFacadeFactory groupFacadeFactory = (group, log) -> rootFacade;
        final ParameterContextFacadeFactory parameterContextFacadeFactory = group -> null;
        final MutableConnectorConfigurationContext activeConfigurationContext = new StandardConnectorConfigurationContext(assetManager, secretsManager);
        final AtomicBoolean failWorkingContextCreation = new AtomicBoolean();
        final FlowContextFactory flowContextFactory = new FlowContextFactory() {
            @Override
            public FrameworkFlowContext createActiveFlowContext(final String connectorId, final ComponentLog connectorLogger, final Bundle bundle) {
                return new StandardFlowContext(processGroup, activeConfigurationContext,
                    groupFacadeFactory, parameterContextFacadeFactory, connectorLogger, FlowContextType.ACTIVE, bundle);
            }

            @Override
            public FrameworkFlowContext createWorkingFlowContext(final String connectorId, final ComponentLog connectorLogger,
                    final MutableConnectorConfigurationContext currentConfiguration, final Bundle bundle) {
                if (failWorkingContextCreation.compareAndSet(true, false)) {
                    throw new IllegalStateException("Expected working context recreation failure");
                }

                return new StandardFlowContext(processGroup, currentConfiguration.clone(), groupFacadeFactory, parameterContextFacadeFactory,
                    connectorLogger, FlowContextType.WORKING, bundle);
            }
        };

        final BundleCoordinate coordinate = new BundleCoordinate("org.apache.nifi", "test-nar", "2.0.0");
        final MockComponentLog componentLog = new MockComponentLog("RecordingConnector", connector);
        final ConnectorDetails details = new ConnectorDetails(connector, coordinate, componentLog);
        final FlowManager flowManager = mock(FlowManager.class);
        when(flowManager.getControllerServiceProvider()).thenReturn(mock(ControllerServiceProvider.class));
        final PropertyEncryptionProvider propertyEncryptionProvider = new ReversingPropertyEncryptionProvider();
        when(flowManager.getPropertyEncryptionProvider()).thenReturn(propertyEncryptionProvider);
        final StandardConnectorNode node = new StandardConnectorNode("connector-1", flowManager, mock(ExtensionManager.class),
            new MockStateManagerProvider(), null, details, "RecordingConnector", connector.getClass().getCanonicalName(),
            activeConfigurationContext, new StandardConnectorStateTransition("RecordingConnector"),
            flowContextFactory, new ConnectorValidationTrigger() {
                @Override
                public void triggerAsync(final ConnectorNode connectorNode) {
                    connectorNode.performValidation();
                }

                @Override
                public void trigger(final ConnectorNode connectorNode) {
                    connectorNode.performValidation();
                }
            }, extensionMissing);
        final FrameworkConnectorInitializationContext initializationContext = mock(FrameworkConnectorInitializationContext.class);
        when(initializationContext.getSecretsManager()).thenReturn(secretsManager);
        when(initializationContext.getAssetManager()).thenReturn(assetManager);
        node.initializeConnector(initializationContext);
        node.loadInitialFlow();

        final StandardConnectorRepository repository = new StandardConnectorRepository();
        final ConnectorRepositoryInitializationContext initContext = mock(ConnectorRepositoryInitializationContext.class);
        when(initContext.getFlowManager()).thenReturn(mock(FlowManager.class));
        when(initContext.getExtensionManager()).thenReturn(mock(ExtensionManager.class));
        when(initContext.getAssetManager()).thenReturn(assetManager);
        when(initContext.getConnectorSyncTimeout()).thenReturn(Duration.ofSeconds(5));
        final ConnectorRequestReplicator requestReplicator;
        if (explicitFlowEvolutionBarrier) {
            final FlowEvolutionRequestReplicator flowEvolutionRequestReplicator = mock(FlowEvolutionRequestReplicator.class);
            when(flowEvolutionRequestReplicator.getFlowEvolutionStatus(anyString(), any(Duration.class))).thenAnswer(invocation -> node.getFlowEvolutionStatus());
            requestReplicator = flowEvolutionRequestReplicator;
        } else {
            requestReplicator = mock(ConnectorRequestReplicator.class);
        }
        when(initContext.getRequestReplicator()).thenReturn(requestReplicator);
        repository.initialize(initContext);

        return new Harness(connector, node, repository, processGroup, lifecycle, componentLog,
            flowManager, failWorkingContextCreation, requestReplicator, propertyEncryptionProvider);
    }

    static final class Harness {
        final RecordingConnector connector;
        final StandardConnectorNode node;
        final StandardConnectorRepository repository;
        final ProcessGroup processGroup;
        final ProcessGroupLifecycle lifecycle;
        final MockComponentLog componentLog;
        final FlowManager flowManager;
        final AtomicBoolean failWorkingContextCreation;
        final ConnectorRequestReplicator requestReplicator;
        final PropertyEncryptionProvider propertyEncryptionProvider;

        private Harness(final RecordingConnector connector, final StandardConnectorNode node, final StandardConnectorRepository repository,
                final ProcessGroup processGroup, final ProcessGroupLifecycle lifecycle,
                final MockComponentLog componentLog, final FlowManager flowManager,
                final AtomicBoolean failWorkingContextCreation,
                final ConnectorRequestReplicator requestReplicator, final PropertyEncryptionProvider propertyEncryptionProvider) {
            this.connector = connector;
            this.node = node;
            this.repository = repository;
            this.processGroup = processGroup;
            this.lifecycle = lifecycle;
            this.componentLog = componentLog;
            this.flowManager = flowManager;
            this.failWorkingContextCreation = failWorkingContextCreation;
            this.requestReplicator = requestReplicator;
            this.propertyEncryptionProvider = propertyEncryptionProvider;
        }

        Bundle loadedBundle() {
            final BundleCoordinate coordinate = node.getBundleCoordinate();
            return new Bundle(coordinate.getGroup(), coordinate.getId(), coordinate.getVersion());
        }
    }

    static final class RecordingConnector extends AbstractConnector {
        volatile FlowEvolutionContext context;
        volatile int evolutionCalls;
        volatile boolean block;
        volatile boolean ignoreHookInterrupt;
        volatile boolean incompleteEvolutionFuture;
        volatile CompletableFuture<Void> evolutionFuture;
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch hookReleased = new CountDownLatch(1);
        final CountDownLatch evolutionFuturePublished = new CountDownLatch(1);

        @Override
        public VersionedExternalFlow getInitialFlow() {
            return null;
        }

        @Override
        public VersionedExternalFlow getActiveFlow(final FlowContext flowContext) {
            final VersionedExternalFlow flow = new VersionedExternalFlow();
            flow.setFlowContents(new VersionedProcessGroup());
            return flow;
        }

        @Override
        public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        public List<ConfigurationStep> getConfigurationSteps() {
            return List.of();
        }

        @Override
        public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        public void start(final FlowContext activeContext) {
        }

        @Override
        protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
        }

        @Override
        public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides,
                final FlowContext flowContext) {
            return List.of();
        }

        @Override
        public CompletableFuture<Void> prepareForFlowEvolution(final FlowEvolutionContext evolutionContext) throws FlowUpdateException {
            context = evolutionContext;
            evolutionCalls++;
            started.countDown();
            if (block) {
                while (true) {
                    try {
                        if (ignoreHookInterrupt) {
                            hookReleased.await();
                        } else {
                            Thread.sleep(60_000);
                        }

                        break;
                    } catch (final InterruptedException e) {
                        if (ignoreHookInterrupt) {
                            continue;
                        }

                        Thread.currentThread().interrupt();
                        throw new FlowUpdateException("interrupted", e);
                    }
                }
            }

            if (incompleteEvolutionFuture) {
                evolutionFuture = new CompletableFuture<>();
            } else {
                evolutionFuture = CompletableFuture.completedFuture(null);
            }
            evolutionFuturePublished.countDown();
            return evolutionFuture;
        }
    }

    private static final class ReversingPropertyEncryptionProvider implements PropertyEncryptionProvider {
        @Override
        public void initialize(final PropertyEncryptionProviderInitializationContext context) {
        }

        @Override
        public byte[] encrypt(final byte[] property, final SensitivePropertyContext context) {
            return reverse(property);
        }

        @Override
        public byte[] decrypt(final byte[] encryptedProperty, final SensitivePropertyContext context) {
            return reverse(encryptedProperty);
        }

        private byte[] reverse(final byte[] input) {
            final byte[] reversed = input.clone();
            for (int left = 0, right = reversed.length - 1; left < right; left++, right--) {
                final byte value = reversed[left];
                reversed[left] = reversed[right];
                reversed[right] = value;
            }

            return reversed;
        }
    }
}
