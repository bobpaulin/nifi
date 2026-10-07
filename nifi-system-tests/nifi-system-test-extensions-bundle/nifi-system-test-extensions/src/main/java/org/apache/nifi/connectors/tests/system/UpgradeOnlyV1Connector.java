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
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Present only in the system-test extensions NAR. After that NAR is removed the connector loads as a ghost.
 */
public class UpgradeOnlyV1Connector extends AbstractConnector {

    public static final String GATE_FILE_PATH_NAME = "Gate File Path";

    static final ConnectorPropertyDescriptor GATE_FILE_PATH = new ConnectorPropertyDescriptor.Builder()
        .name(GATE_FILE_PATH_NAME)
        .description("Relative path of the file that lets the terminating processor run.")
        .required(true)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConfigurationStep CONFIG_STEP = new ConfigurationStep.Builder()
        .name("Gate")
        .propertyGroups(List.of(new ConnectorPropertyGroup.Builder().name("Gate").addProperty(GATE_FILE_PATH).build()))
        .build();

    private static final Bundle GATE_BUNDLE = new Bundle("org.apache.nifi", "nifi-system-test-extensions2-nar", "2.13.0-SNAPSHOT");

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
        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup("upgrade-only-v1-root", "Upgrade Only V1");
        final VersionedProcessor generate = VersionedFlowUtils.addProcessor(rootGroup,
            "org.apache.nifi.processors.tests.system.UpgradeDrainGenerate", GATE_BUNDLE, "GenerateFlowFile", new Position(0, 0));
        generate.getProperties().put("File Size", "1 B");
        generate.setSchedulingPeriod("100 millis");
        final VersionedProcessor terminate = VersionedFlowUtils.addProcessor(rootGroup,
            "org.apache.nifi.processors.tests.system.UpgradeDrainTerminate", GATE_BUNDLE, "TerminateFlowFile", new Position(400, 0));
        if (flowContext != null) {
            final String gateFile = flowContext.getConfigurationContext().getProperty(CONFIG_STEP, GATE_FILE_PATH).getValue();
            if (gateFile != null) {
                terminate.getProperties().put("Gate File", gateFile);
            }
        }
        VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(generate),
            VersionedFlowUtils.createConnectableComponent(terminate), Set.of("success"));

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        return flow;
    }
}
