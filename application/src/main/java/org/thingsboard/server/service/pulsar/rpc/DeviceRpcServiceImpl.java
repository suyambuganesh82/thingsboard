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
package org.thingsboard.server.service.pulsar.rpc;

import io.triveni.platform.rpc.device.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.credentials.BasicMqttCredentials;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UUIDBased;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.DeviceCredentialsType;
import org.thingsboard.server.common.msg.EncryptionUtil;
import org.thingsboard.server.dao.device.DeviceCredentialsService;
import org.thingsboard.server.dao.device.DeviceProfileService;
import org.thingsboard.server.dao.device.DeviceService;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.rpc.DeviceRpcService;
import org.thingsboard.server.service.profile.TbDeviceProfileCache;
import org.thingsboard.server.service.transport.BasicCredentialsValidationResult;

import java.util.Arrays;
import java.util.UUID;

import static org.thingsboard.server.common.data.security.DeviceCredentialsType.MQTT_BASIC;
import static org.thingsboard.server.service.transport.BasicCredentialsValidationResult.PASSWORD_MISMATCH;
import static org.thingsboard.server.service.transport.BasicCredentialsValidationResult.VALID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceRpcServiceImpl implements DeviceRpcService {

    private final TbDeviceProfileCache deviceProfileCache;
    private final TbTenantProfileCache tenantProfileCache;
    private final DeviceService deviceService;
    private final DeviceProfileService deviceProfileService;
    private final DeviceCredentialsService deviceCredentialsService;

    public static <T> T notNull(T value, T defaultValue) {
        return value != null ? value : defaultValue;
    }

    public static UUID ofUuid(UUIDBased value) {
        return value != null ? value.getId() : null;
    }

    private static boolean checkIsMqttCredentials(DeviceCredentials credentials) {
        return credentials != null && MQTT_BASIC.equals(credentials.getCredentialsType());
    }

    @Override
    public ValidateMqttAuthEvent process(ValidateMqttAuthCommand command) {

        DeviceCredentials credentials;
        if (StringUtils.isEmpty(command.getUsername())) {
            credentials = checkMqttCredentials(command, EncryptionUtil.getSha3Hash(command.getClientId()));
            if (credentials != null) {
                return getDeviceInfo(credentials);
            } else {
                return getFailedEventResponse();
            }
        } else {
            credentials = deviceCredentialsService.findDeviceCredentialsByCredentialsId(EncryptionUtil.getSha3Hash("|", command.getClientId(), command.getUsername()));
            if (checkIsMqttCredentials(credentials)) {
                var validationResult = validateMqttCredentials(command, credentials);
                if (VALID.equals(validationResult)) {
                    return getDeviceInfo(credentials);
                } else if (PASSWORD_MISMATCH.equals(validationResult)) {
                    return getFailedEventResponse();
                } else {
                    return validateUserNameCredentials(command);
                }
            } else {
                return validateUserNameCredentials(command);
            }
        }
    }

    private ValidateMqttAuthEvent validateUserNameCredentials(ValidateMqttAuthCommand command) {
        DeviceCredentials credentials = deviceCredentialsService.findDeviceCredentialsByCredentialsId(command.getUsername());
        if (credentials != null) {
            switch (credentials.getCredentialsType()) {
                case ACCESS_TOKEN:
                    return getDeviceInfo(credentials);
                case MQTT_BASIC:
                    if (VALID.equals(validateMqttCredentials(command, credentials))) {
                        return getDeviceInfo(credentials);
                    } else {
                        return getFailedEventResponse();
                    }
            }
        }
        return getFailedEventResponse();
    }

    private DeviceCredentials checkMqttCredentials(ValidateMqttAuthCommand command, String credId) {
        DeviceCredentials deviceCredentials = deviceCredentialsService.findDeviceCredentialsByCredentialsId(credId);
        if (deviceCredentials != null && deviceCredentials.getCredentialsType() == MQTT_BASIC) {
            if (VALID.equals(validateMqttCredentials(command, deviceCredentials))) {
                return deviceCredentials;
            }
        }
        return null;
    }

    private BasicCredentialsValidationResult validateMqttCredentials(ValidateMqttAuthCommand command, DeviceCredentials deviceCredentials) {
        BasicMqttCredentials dbCred = JacksonUtil.fromString(deviceCredentials.getCredentialsValue(), BasicMqttCredentials.class);
        if (!StringUtils.isEmpty(dbCred.getClientId()) && !dbCred.getClientId().equals(command.getClientId())) {
            return BasicCredentialsValidationResult.HASH_MISMATCH;
        }
        if (!StringUtils.isEmpty(dbCred.getUserName()) && !dbCred.getUserName().equals(command.getUsername())) {
            return BasicCredentialsValidationResult.HASH_MISMATCH;
        }
        if (!StringUtils.isEmpty(dbCred.getPassword())) {
            if (StringUtils.isEmpty(command.getPassword())) {
                return BasicCredentialsValidationResult.PASSWORD_MISMATCH;
            } else {
                return dbCred.getPassword().equals(command.getPassword()) ? VALID : BasicCredentialsValidationResult.PASSWORD_MISMATCH;
            }
        }
        return VALID;
    }

    ValidateMqttAuthEvent getDeviceInfo(DeviceCredentials credentials) {
        ValidateMqttAuthEventBuilder builder = ValidateMqttAuthEventBuilder.validateMqttAuthEvent();
        Device device = deviceService.findDeviceById(TenantId.SYS_TENANT_ID, credentials.getDeviceId());
        if (device == null) {
            log.trace("[{}] Failed to lookup device by id", credentials.getDeviceId());
            return getFailedEventResponse();
        }
        builder.deviceInfoModel(buildDeviceInfoModel(device));
        DeviceProfile deviceProfile = deviceProfileCache.get(device.getTenantId(), device.getDeviceProfileId());
        if (deviceProfile != null) {
            builder.deviceProfileModel(buildDeviceProfileModel(deviceProfile));
        } else {
            log.warn("[{}] Failed to find device profile [{}] for device. ", device.getId(), device.getDeviceProfileId());
        }
        if (!StringUtils.isEmpty(credentials.getCredentialsValue()) &&
                MQTT_BASIC.equals(credentials.getCredentialsType())) {
            DeviceCredential deviceCredential = JacksonUtil.fromString(credentials.getCredentialsValue(), DeviceCredential.class);
            builder.deviceCredential(deviceCredential);
        }
        return builder.status("SUCCESS").build();
    }

    private ValidateMqttAuthEvent getFailedEventResponse() {
        return ValidateMqttAuthEventBuilder.validateMqttAuthEvent()
                .status("FAILED")
                .build();
    }

    private DeviceProfileModel buildDeviceProfileModel(DeviceProfile deviceProfile) {
        return DeviceProfileModelBuilder.deviceProfileModel()
                .tenantId(deviceProfile.getTenantId().getId())
                .deviceProfileId(deviceProfile.getId().getId())
                .createdTime(deviceProfile.getCreatedTime())
                .name(deviceProfile.getName())
                .isDefault(deviceProfile.isDefault())
                .type(deviceProfile.getType().name())
                .transportType(deviceProfile.getTransportType().name())
                .provisionType(deviceProfile.getProvisionType().name())
                .deviceProfileData(getStringFromBytes(deviceProfile.getProfileDataBytes()))
                .description(notNull(deviceProfile.getDescription(), null))
                .image(notNull(deviceProfile.getImage(), null))
                .defaultRuleChainId(ofUuid(deviceProfile.getDefaultRuleChainId()))
                .defaultDashboardId(ofUuid(deviceProfile.getDefaultDashboardId()))
                .defaultQueueName(notNull(deviceProfile.getDefaultQueueName(), null))
                .provisionDeviceKey(notNull(deviceProfile.getProvisionDeviceKey(), null))
                .firmwareId(ofUuid(deviceProfile.getFirmwareId()))
                .softwareId(ofUuid(deviceProfile.getSoftwareId()))
                .externalId(ofUuid(deviceProfile.getExternalId()))
                .build();
    }

    private String getStringFromBytes(byte[] profileDataBytes) {
        if (null != profileDataBytes) {
            return Arrays.toString(profileDataBytes);
        }
        return null;
    }

    private DeviceInfoModel buildDeviceInfoModel(Device device) {
        return DeviceInfoModelBuilder.deviceInfoModel()
                .deviceId(device.getId().getId())
                .deviceName(device.getName())
                .deviceType(device.getType())
                .deviceProfileId(device.getDeviceProfileId().getId())
                .tenantId(device.getTenantId().getId())
                .customerId(ofUuid(device.getCustomerId()))
                .additionalInfo(device.getAdditionalInfo())
                .build();
    }
}
