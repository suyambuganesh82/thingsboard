/**
 * Copyright © 2016-2024 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.device;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.credentials.BasicMqttCredentials;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.DeviceCredentialsType;
import org.thingsboard.server.common.data.sync.ie.importing.csv.BulkImportColumnType;
import org.thingsboard.server.dao.device.DeviceCredentialsService;
import org.thingsboard.server.dao.device.DeviceProfileService;
import org.thingsboard.server.dao.device.DeviceService;
import org.thingsboard.server.dao.exception.DeviceCredentialsValidationException;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.entitiy.device.TbDeviceService;
import org.thingsboard.server.service.security.model.SecurityUser;
import org.thingsboard.server.service.sync.ie.importing.csv.AbstractBulkImportService;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Service
@TbCoreComponent
@RequiredArgsConstructor
public class DeviceBulkImportService extends AbstractBulkImportService<Device> {
    protected final DeviceService deviceService;
    protected final TbDeviceService tbDeviceService;
    protected final DeviceCredentialsService deviceCredentialsService;
    protected final DeviceProfileService deviceProfileService;

    private final Lock findOrCreateDeviceProfileLock = new ReentrantLock();

    @Override
    protected void setEntityFields(Device device, Map<BulkImportColumnType, String> fields) {
        ObjectNode additionalInfo = getOrCreateAdditionalInfoObj(device);
        fields.forEach((columnType, value) -> {
            switch (columnType) {
                case NAME:
                    device.setName(value);
                    break;
                case TYPE:
                    device.setType(value);
                    break;
                case LABEL:
                    device.setLabel(value);
                    break;
                case DESCRIPTION:
                    additionalInfo.set("description", new TextNode(value));
                    break;
                case IS_GATEWAY:
                    additionalInfo.set("gateway", BooleanNode.valueOf(Boolean.parseBoolean(value)));
                    break;
            }
            device.setAdditionalInfo(additionalInfo);
        });
    }

    @Override
    @SneakyThrows
    protected Device saveEntity(SecurityUser user, Device device, Map<BulkImportColumnType, String> fields) {
        DeviceCredentials deviceCredentials;
        try {
            deviceCredentials = createDeviceCredentials(device.getTenantId(), device.getId(), fields);
            deviceCredentialsService.formatCredentials(deviceCredentials);
        } catch (Exception e) {
            throw new DeviceCredentialsValidationException("Invalid device credentials: " + e.getMessage());
        }

        DeviceProfile deviceProfile;
        if (StringUtils.isNotEmpty(device.getType())) {
            deviceProfile = deviceProfileService.findOrCreateDeviceProfile(device.getTenantId(), device.getType());
        } else {
            deviceProfile = deviceProfileService.findDefaultDeviceProfile(device.getTenantId());
        }
        device.setDeviceProfileId(deviceProfile.getId());

        return tbDeviceService.saveDeviceWithCredentials(device, deviceCredentials, user);
    }

    @Override
    protected Device findOrCreateEntity(TenantId tenantId, String name) {
        return Optional.ofNullable(deviceService.findDeviceByTenantIdAndName(tenantId, name))
                .orElseGet(Device::new);
    }

    @Override
    protected void setOwners(Device entity, SecurityUser user) {
        entity.setTenantId(user.getTenantId());
        entity.setCustomerId(user.getCustomerId());
    }

    @SneakyThrows
    private DeviceCredentials createDeviceCredentials(TenantId tenantId, DeviceId deviceId, Map<BulkImportColumnType, String> fields) {
        DeviceCredentials credentials = new DeviceCredentials();
        if (fields.containsKey(BulkImportColumnType.X509)) {
            credentials.setCredentialsType(DeviceCredentialsType.X509_CERTIFICATE);
            setUpX509CertificateCredentials(fields, credentials);
        } else if (CollectionUtils.containsAny(fields.keySet(), EnumSet.of(BulkImportColumnType.MQTT_CLIENT_ID, BulkImportColumnType.MQTT_USER_NAME, BulkImportColumnType.MQTT_PASSWORD))) {
            credentials.setCredentialsType(DeviceCredentialsType.MQTT_BASIC);
            setUpBasicMqttCredentials(fields, credentials);
        } else if (deviceId != null && !fields.containsKey(BulkImportColumnType.ACCESS_TOKEN)) {
            credentials = deviceCredentialsService.findDeviceCredentialsByDeviceId(tenantId, deviceId);
        } else {
            credentials.setCredentialsType(DeviceCredentialsType.ACCESS_TOKEN);
            setUpAccessTokenCredentials(fields, credentials);
        }
        return credentials;
    }

    private void setUpAccessTokenCredentials(Map<BulkImportColumnType, String> fields, DeviceCredentials credentials) {
        credentials.setCredentialsId(Optional.ofNullable(fields.get(BulkImportColumnType.ACCESS_TOKEN))
                .orElseGet(() -> StringUtils.randomAlphanumeric(20)));
    }

    private void setUpBasicMqttCredentials(Map<BulkImportColumnType, String> fields, DeviceCredentials credentials) {
        BasicMqttCredentials basicMqttCredentials = new BasicMqttCredentials();
        basicMqttCredentials.setClientId(fields.get(BulkImportColumnType.MQTT_CLIENT_ID));
        basicMqttCredentials.setUserName(fields.get(BulkImportColumnType.MQTT_USER_NAME));
        basicMqttCredentials.setPassword(fields.get(BulkImportColumnType.MQTT_PASSWORD));
        credentials.setCredentialsValue(JacksonUtil.toString(basicMqttCredentials));
    }

    private void setUpX509CertificateCredentials(Map<BulkImportColumnType, String> fields, DeviceCredentials credentials) {
        credentials.setCredentialsValue(fields.get(BulkImportColumnType.X509));
    }

    @Override
    protected EntityType getEntityType() {
        return EntityType.DEVICE;
    }

}
