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

import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.security.encryption.PropertyEncryptionEncoder;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;
import org.apache.nifi.security.encryption.PropertyEncryptionProviderInitializationContext;
import org.apache.nifi.security.encryption.SensitivePropertyCodec;
import org.apache.nifi.security.encryption.SensitivePropertyContext;
import org.apache.nifi.security.encryption.SensitivePropertyContextFactory;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStandardFlowContext {

    @Test
    void testReloadReplacesStateAfterCreatingRootGroup() {
        final ProcessGroup managedProcessGroup = mock(ProcessGroup.class);
        final ProcessGroupFacadeFactory groupFacadeFactory = mock(ProcessGroupFacadeFactory.class);
        final ParameterContextFacadeFactory parameterContextFacadeFactory = mock(ParameterContextFacadeFactory.class);
        final ComponentLog originalLog = mock(ComponentLog.class);
        final ComponentLog replacementLog = mock(ComponentLog.class);
        final MutableConnectorConfigurationContext originalConfiguration = mock(MutableConnectorConfigurationContext.class);
        final MutableConnectorConfigurationContext replacementConfiguration = mock(MutableConnectorConfigurationContext.class);
        final MutableConnectorConfigurationContext failedConfiguration = mock(MutableConnectorConfigurationContext.class);
        final Bundle originalBundle = new Bundle("group", "artifact", "1.0.0");
        final Bundle replacementBundle = new Bundle("group", "artifact", "2.0.0");
        final Bundle failedBundle = new Bundle("group", "artifact", "3.0.0");
        final ProcessGroupFacade originalRootGroup = mock(ProcessGroupFacade.class);
        final ProcessGroupFacade replacementRootGroup = mock(ProcessGroupFacade.class);
        when(groupFacadeFactory.create(managedProcessGroup, originalLog)).thenReturn(originalRootGroup);
        when(groupFacadeFactory.create(managedProcessGroup, replacementLog)).thenReturn(replacementRootGroup);

        final StandardFlowContext flowContext = new StandardFlowContext(managedProcessGroup, originalConfiguration, groupFacadeFactory,
            parameterContextFacadeFactory, originalLog, FlowContextType.ACTIVE, originalBundle);

        flowContext.reload(replacementBundle, replacementLog, replacementConfiguration);

        assertSame(replacementBundle, flowContext.getBundle());
        assertSame(replacementConfiguration, flowContext.getConfigurationContext());
        assertSame(replacementRootGroup, flowContext.getRootGroup());

        final ComponentLog failedLog = mock(ComponentLog.class);
        when(groupFacadeFactory.create(managedProcessGroup, failedLog)).thenThrow(new IllegalStateException("Root group creation failed"));

        assertThrows(IllegalStateException.class, () -> flowContext.reload(failedBundle, failedLog, failedConfiguration));
        assertSame(replacementBundle, flowContext.getBundle());
        assertSame(replacementConfiguration, flowContext.getConfigurationContext());
        assertSame(replacementRootGroup, flowContext.getRootGroup());
    }

    @Test
    void testRestorePersistedFlowWithoutResumingLeavesComponentsStopped() {
        final ProcessGroup managedProcessGroup = mock(ProcessGroup.class);
        final StandardFlowContext flowContext = new StandardFlowContext(managedProcessGroup, mock(MutableConnectorConfigurationContext.class),
            mock(ProcessGroupFacadeFactory.class), mock(ParameterContextFacadeFactory.class), mock(ComponentLog.class), FlowContextType.ACTIVE, new Bundle());
        final VersionedProcessGroup persistedFlow = new VersionedProcessGroup();

        flowContext.restorePersistedFlow(persistedFlow, null, false);

        verify(managedProcessGroup).restoreFlowPreservingIdentifiers(argThat(flow -> flow.getFlowContents() == persistedFlow), eq(false));
        verify(managedProcessGroup, never()).restoreFlowPreservingIdentifiers(any());
    }

    @Test
    void testRestorePersistedFlowDefaultsToProcessGroupResumeSetting() {
        final ProcessGroup managedProcessGroup = mock(ProcessGroup.class);
        final StandardFlowContext flowContext = new StandardFlowContext(managedProcessGroup, mock(MutableConnectorConfigurationContext.class),
            mock(ProcessGroupFacadeFactory.class), mock(ParameterContextFacadeFactory.class), mock(ComponentLog.class), FlowContextType.ACTIVE, new Bundle());
        final VersionedProcessGroup persistedFlow = new VersionedProcessGroup();

        flowContext.restorePersistedFlow(persistedFlow, null);

        verify(managedProcessGroup).restoreFlowPreservingIdentifiers(argThat(flow -> flow.getFlowContents() == persistedFlow));
        verify(managedProcessGroup, never()).restoreFlowPreservingIdentifiers(any(), anyBoolean());
    }

    @Test
    public void testRestorePersistedFlowResolvesParameterAssets() {
        final Asset asset = mock(Asset.class);
        when(asset.getFile()).thenReturn(new File("target/asset.txt"));
        final AssetManager assetManager = mock(AssetManager.class);
        when(assetManager.getAsset("asset-1")).thenReturn(Optional.of(asset));

        final ParameterContext managedParameterContext = mock(ParameterContext.class);
        when(managedParameterContext.getName()).thenReturn("managed-parameters");
        final ProcessGroup managedProcessGroup = mock(ProcessGroup.class);
        when(managedProcessGroup.getParameterContext()).thenReturn(managedParameterContext);

        final ParameterContextFacade parameterContextFacade = mock(ParameterContextFacade.class);
        final PropertyEncryptionProvider propertyEncryptionProvider = new ReversingPropertyEncryptionProvider();
        final StandardFlowContext flowContext = new StandardFlowContext(managedProcessGroup,
            mock(MutableConnectorConfigurationContext.class), assetManager, propertyEncryptionProvider, mock(ProcessGroupFacadeFactory.class),
            group -> parameterContextFacade, mock(ComponentLog.class), FlowContextType.ACTIVE, new Bundle());

        final VersionedAsset versionedAsset = new VersionedAsset();
        versionedAsset.setIdentifier("asset-1");
        versionedAsset.setName("asset.txt");
        final VersionedParameter versionedParameter = new VersionedParameter();
        versionedParameter.setName("Asset Parameter");
        versionedParameter.setReferencedAssets(List.of(versionedAsset));
        final VersionedParameter sensitiveParameter = new VersionedParameter();
        sensitiveParameter.setName("Sensitive Parameter");
        sensitiveParameter.setSensitive(true);
        final String encryptedValue = SensitivePropertyCodec.encrypt(propertyEncryptionProvider, "sensitive-value",
            SensitivePropertyContextFactory.forParameter("managed-parameters", "Sensitive Parameter"));
        sensitiveParameter.setValue(PropertyEncryptionEncoder.getEncoded(encryptedValue));
        final VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
        versionedParameterContext.setIdentifier("parameter-context-1");
        versionedParameterContext.setName("managed-parameters");
        versionedParameterContext.setParameters(Set.of(versionedParameter, sensitiveParameter));

        flowContext.restorePersistedFlow(new VersionedProcessGroup(), versionedParameterContext);

        final ArgumentCaptor<List<ParameterValue>> valuesCaptor = ArgumentCaptor.forClass(List.class);
        verify(parameterContextFacade).updateParameters(valuesCaptor.capture());
        final List<ParameterValue> restoredParameters = valuesCaptor.getValue();
        final ParameterValue restoredAssetParameter = restoredParameters.stream()
            .filter(parameter -> "Asset Parameter".equals(parameter.getName()))
            .findFirst()
            .orElseThrow();
        final ParameterValue restoredSensitiveParameter = restoredParameters.stream()
            .filter(parameter -> "Sensitive Parameter".equals(parameter.getName()))
            .findFirst()
            .orElseThrow();
        assertSame(asset, restoredAssetParameter.getAssets().getFirst());
        assertEquals("sensitive-value", restoredSensitiveParameter.getValue());
    }

    private static class ReversingPropertyEncryptionProvider implements PropertyEncryptionProvider {
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
