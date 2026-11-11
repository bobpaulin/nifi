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
package org.apache.nifi.cs.tests.system;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.AbstractControllerService;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Writes a random marker to its local state the first time it is enabled and never changes it afterwards, so tests
 * can detect whether the service's state was cleared.
 */
@Stateful(scopes = Scope.LOCAL, description = "Stores a random marker under the key " + UpgradeDrainMarkerService.MARKER_KEY)
public class UpgradeDrainMarkerService extends AbstractControllerService {

    public static final String MARKER_KEY = "marker";

    @OnEnabled
    public void writeMarker() throws IOException {
        final StateManager stateManager = getStateManager();
        if (stateManager.getState(Scope.LOCAL).get(MARKER_KEY) == null) {
            stateManager.setState(Map.of(MARKER_KEY, UUID.randomUUID().toString()), Scope.LOCAL);
        }
    }
}
