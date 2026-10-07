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
package org.apache.nifi.web.api.dto;

import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class EntityFactoryTest {

    @Test
    void testCreateProcessGroupEntityPromotesResolvedExecutionEngineWhenUnauthorized() {
        final ProcessGroupDTO dto = new ProcessGroupDTO();
        dto.setId("pg-1");
        dto.setResolvedExecutionEngine("STATELESS");

        final PermissionsDTO permissions = new PermissionsDTO();
        permissions.setCanRead(false);
        permissions.setCanWrite(false);

        final ProcessGroupEntity entity = new EntityFactory().createProcessGroupEntity(dto, null, permissions, null, null);

        assertEquals("STATELESS", entity.getResolvedExecutionEngine());
        assertNull(entity.getComponent());
    }

    @Test
    void testCreateProcessGroupEntityIncludesComponentWhenAuthorized() {
        final ProcessGroupDTO dto = new ProcessGroupDTO();
        dto.setId("pg-1");
        dto.setResolvedExecutionEngine("STANDARD");

        final PermissionsDTO permissions = new PermissionsDTO();
        permissions.setCanRead(true);
        permissions.setCanWrite(true);

        final ProcessGroupEntity entity = new EntityFactory().createProcessGroupEntity(dto, null, permissions, null, null);

        assertEquals("STANDARD", entity.getResolvedExecutionEngine());
        assertEquals(dto, entity.getComponent());
    }

    @Test
    void testCreateConnectorEntityIncludesBulletinsWhenAuthorized() {
        final ConnectorDTO dto = new ConnectorDTO();
        dto.setId("connector-1");
        final PermissionsDTO permissions = new PermissionsDTO();
        permissions.setCanRead(true);
        final List<BulletinEntity> bulletins = List.of(new BulletinEntity());

        final ConnectorEntity entity = new EntityFactory().createConnectorEntity(dto, null, permissions, null, null, bulletins);

        assertSame(bulletins, entity.getBulletins());
    }

    @Test
    void testCreateConnectorEntityExcludesBulletinsWhenUnauthorized() {
        final ConnectorDTO dto = new ConnectorDTO();
        dto.setId("connector-1");
        final PermissionsDTO permissions = new PermissionsDTO();
        permissions.setCanRead(false);

        final ConnectorEntity entity = new EntityFactory().createConnectorEntity(dto, null, permissions, null, null, List.of(new BulletinEntity()));

        assertNull(entity.getBulletins());
    }
}
