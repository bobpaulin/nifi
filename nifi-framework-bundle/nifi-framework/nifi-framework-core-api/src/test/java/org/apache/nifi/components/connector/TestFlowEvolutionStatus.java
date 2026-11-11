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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.nifi.components.connector.FlowEvolutionStatus.EVOLVE_DRAIN_COMPLETE;
import static org.apache.nifi.components.connector.FlowEvolutionStatus.EVOLVE_DRAIN_PENDING;
import static org.apache.nifi.components.connector.FlowEvolutionStatus.FAILED;
import static org.apache.nifi.components.connector.FlowEvolutionStatus.INACTIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestFlowEvolutionStatus {

    @Test
    void testMerge() {
        assertEquals(INACTIVE, FlowEvolutionStatus.merge(null));
        assertEquals(INACTIVE, FlowEvolutionStatus.merge(List.of()));
        assertEquals(INACTIVE, FlowEvolutionStatus.merge(List.of(INACTIVE, INACTIVE)));

        assertEquals(EVOLVE_DRAIN_COMPLETE, FlowEvolutionStatus.merge(Arrays.asList(INACTIVE, null, EVOLVE_DRAIN_COMPLETE)));
        assertEquals(EVOLVE_DRAIN_PENDING, FlowEvolutionStatus.merge(List.of(EVOLVE_DRAIN_COMPLETE, EVOLVE_DRAIN_PENDING, EVOLVE_DRAIN_COMPLETE)));

        assertEquals(FAILED, FlowEvolutionStatus.merge(List.of(EVOLVE_DRAIN_PENDING, FAILED, INACTIVE)));
        assertEquals(FAILED, FlowEvolutionStatus.merge(List.of(FAILED, EVOLVE_DRAIN_PENDING)));
    }
}
