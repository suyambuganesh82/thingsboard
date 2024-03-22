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
package org.thingsboard.server.dao.service.validator;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.thingsboard.server.common.data.DashboardInfo;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceProfileProvisionType;
import org.thingsboard.server.common.data.DynamicProtoUtils;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.credentials.lwm2m.LwM2MSecurityMode;
import org.thingsboard.server.common.data.device.profile.CoapDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.CoapDeviceTypeConfiguration;
import org.thingsboard.server.common.data.device.profile.DefaultCoapDeviceTypeConfiguration;
import org.thingsboard.server.common.data.device.profile.DeviceProfileAlarm;
import org.thingsboard.server.common.data.device.profile.DeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.Lwm2mDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.MqttDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.ProtoTransportPayloadConfiguration;
import org.thingsboard.server.common.data.device.profile.TransportPayloadTypeConfiguration;
import org.thingsboard.server.common.data.device.profile.lwm2m.bootstrap.AbstractLwM2MBootstrapServerCredential;
import org.thingsboard.server.common.data.device.profile.lwm2m.bootstrap.LwM2MBootstrapServerCredential;
import org.thingsboard.server.common.data.device.profile.lwm2m.bootstrap.RPKLwM2MBootstrapServerCredential;
import org.thingsboard.server.common.data.device.profile.lwm2m.bootstrap.X509LwM2MBootstrapServerCredential;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.queue.Queue;
import org.thingsboard.server.common.data.rule.RuleChain;
import org.thingsboard.server.common.msg.EncryptionUtil;
import org.thingsboard.server.dao.dashboard.DashboardService;
import org.thingsboard.server.dao.device.DeviceDao;
import org.thingsboard.server.dao.device.DeviceProfileDao;
import org.thingsboard.server.dao.device.DeviceProfileService;
import org.thingsboard.server.dao.exception.DataValidationException;
import org.thingsboard.server.dao.exception.DeviceCredentialsValidationException;
import org.thingsboard.server.dao.queue.QueueService;
import org.thingsboard.server.dao.rule.RuleChainService;
import org.thingsboard.server.dao.tenant.TenantService;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateEncodingException;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Component
public class DeviceProfileDataValidator extends AbstractHasOtaPackageValidator<DeviceProfile> {

    private static final String ATTRIBUTES_PROTO_SCHEMA = "attributes proto schema";
    private static final String TELEMETRY_PROTO_SCHEMA = "telemetry proto schema";
    private static final String RPC_REQUEST_PROTO_SCHEMA = "rpc request proto schema";
    private static final String RPC_RESPONSE_PROTO_SCHEMA = "rpc response proto schema";
    private static final String EXCEPTION_PREFIX = "[Transport Configuration]";

    @Autowired
    private DeviceProfileDao deviceProfileDao;
    @Autowired
    @Lazy
    private DeviceProfileService deviceProfileService;
    @Autowired
    private DeviceDao deviceDao;
    @Autowired
    private TenantService tenantService;
    @Lazy
    @Autowired
    private QueueService queueService;
    @Autowired
    private RuleChainService ruleChainService;
    @Autowired
    private DashboardService dashboardService;

    @Value("${security.java_cacerts.path:}")
    private String javaCacertsPath;

    @Value("${security.java_cacerts.password:}")
    private String javaCacertsPassword;

    @Override
    protected void validateDataImpl(TenantId tenantId, DeviceProfile deviceProfile) {
        validateString("Device profile name", deviceProfile.getName());
        if (deviceProfile.getType() == null) {
            throw new DataValidationException("Device profile type should be specified!");
        }
        if (deviceProfile.getTransportType() == null) {
            throw new DataValidationException("Device profile transport type should be specified!");
        }
        if (deviceProfile.getTenantId() == null) {
            throw new DataValidationException("Device profile should be assigned to tenant!");
        } else {
            if (!tenantService.tenantExists(deviceProfile.getTenantId())) {
                throw new DataValidationException("Device profile is referencing to non-existent tenant!");
            }
        }
        if (deviceProfile.isDefault()) {
            DeviceProfile defaultDeviceProfile = deviceProfileService.findDefaultDeviceProfile(tenantId);
            if (defaultDeviceProfile != null && !defaultDeviceProfile.getId().equals(deviceProfile.getId())) {
                throw new DataValidationException("Another default device profile is present in scope of current tenant!");
            }
        }
        if (StringUtils.isNotEmpty(deviceProfile.getDefaultQueueName())) {
            Queue queue = queueService.findQueueByTenantIdAndName(tenantId, deviceProfile.getDefaultQueueName());
            if (queue == null) {
                throw new DataValidationException("Device profile is referencing to non-existent queue!");
            }
        }
        if (deviceProfile.getProvisionType() == null) {
            deviceProfile.setProvisionType(DeviceProfileProvisionType.DISABLED);
        }
        if (deviceProfile.getProvisionDeviceKey() != null && DeviceProfileProvisionType.X509_CERTIFICATE_CHAIN.equals(deviceProfile.getProvisionType())) {
            if (isDeviceProfileCertificateInJavaCacerts(deviceProfile.getProfileData().getProvisionConfiguration().getProvisionDeviceSecret())) {
                throw new DataValidationException("Device profile certificate cannot be well known root CA!");
            }
        }
        DeviceProfileTransportConfiguration transportConfiguration = deviceProfile.getProfileData().getTransportConfiguration();
        transportConfiguration.validate();
        if (transportConfiguration instanceof MqttDeviceProfileTransportConfiguration mqttTransportConfiguration) {
            if (mqttTransportConfiguration.getTransportPayloadTypeConfiguration() instanceof ProtoTransportPayloadConfiguration protoTransportPayloadConfiguration) {
                validateProtoSchemas(protoTransportPayloadConfiguration);
                validateTelemetryDynamicMessageFields(protoTransportPayloadConfiguration);
                validateRpcRequestDynamicMessageFields(protoTransportPayloadConfiguration);
            }
        }

        List<DeviceProfileAlarm> profileAlarms = deviceProfile.getProfileData().getAlarms();

        if (!CollectionUtils.isEmpty(profileAlarms)) {
            Set<String> alarmTypes = new HashSet<>();
            for (DeviceProfileAlarm alarm : profileAlarms) {
                String alarmType = alarm.getAlarmType();
                if (StringUtils.isEmpty(alarmType)) {
                    throw new DataValidationException("Alarm rule type should be specified!");
                }
                if (!alarmTypes.add(alarmType)) {
                    throw new DataValidationException(String.format("Can't create device profile with the same alarm rule types: \"%s\"!", alarmType));
                }
            }
        }

        if (deviceProfile.getDefaultRuleChainId() != null) {
            RuleChain ruleChain = ruleChainService.findRuleChainById(tenantId, deviceProfile.getDefaultRuleChainId());
            if (ruleChain == null) {
                throw new DataValidationException("Can't assign non-existent rule chain!");
            }
            if (!ruleChain.getTenantId().equals(deviceProfile.getTenantId())) {
                throw new DataValidationException("Can't assign rule chain from different tenant!");
            }
        }

        if (deviceProfile.getDefaultDashboardId() != null) {
            DashboardInfo dashboard = dashboardService.findDashboardInfoById(tenantId, deviceProfile.getDefaultDashboardId());
            if (dashboard == null) {
                throw new DataValidationException("Can't assign non-existent dashboard!");
            }
            if (!dashboard.getTenantId().equals(deviceProfile.getTenantId())) {
                throw new DataValidationException("Can't assign dashboard from different tenant!");
            }
        }

        validateOtaPackage(tenantId, deviceProfile, deviceProfile.getId());
    }

    @Override
    protected DeviceProfile validateUpdate(TenantId tenantId, DeviceProfile deviceProfile) {
        DeviceProfile old = deviceProfileDao.findById(deviceProfile.getTenantId(), deviceProfile.getId().getId());
        if (old == null) {
            throw new DataValidationException("Can't update non existing device profile!");
        }
        boolean profileTypeChanged = !old.getType().equals(deviceProfile.getType());
        boolean transportTypeChanged = !old.getTransportType().equals(deviceProfile.getTransportType());
        if (profileTypeChanged || transportTypeChanged) {
            Long profileDeviceCount = deviceDao.countDevicesByDeviceProfileId(deviceProfile.getTenantId(), deviceProfile.getId().getId());
            if (profileDeviceCount > 0) {
                String message = null;
                if (profileTypeChanged) {
                    message = "Can't change device profile type because devices referenced it!";
                } else if (transportTypeChanged) {
                    message = "Can't change device profile transport type because devices referenced it!";
                }
                throw new DataValidationException(message);
            }
        }
        if (deviceProfile.getProvisionDeviceKey() != null && DeviceProfileProvisionType.X509_CERTIFICATE_CHAIN.equals(deviceProfile.getProvisionType())) {
            if (isDeviceProfileCertificateInJavaCacerts(deviceProfile.getProvisionDeviceKey())) {
                throw new DataValidationException("Device profile certificate cannot be well known root CA!");
            }
        }
        return old;
    }

    private void validateProtoSchemas(ProtoTransportPayloadConfiguration protoTransportPayloadTypeConfiguration) {
        try {
            DynamicProtoUtils.validateProtoSchema(protoTransportPayloadTypeConfiguration.getDeviceAttributesProtoSchema(), ATTRIBUTES_PROTO_SCHEMA, EXCEPTION_PREFIX);
            DynamicProtoUtils.validateProtoSchema(protoTransportPayloadTypeConfiguration.getDeviceTelemetryProtoSchema(), TELEMETRY_PROTO_SCHEMA, EXCEPTION_PREFIX);
            DynamicProtoUtils.validateProtoSchema(protoTransportPayloadTypeConfiguration.getDeviceRpcRequestProtoSchema(), RPC_REQUEST_PROTO_SCHEMA, EXCEPTION_PREFIX);
            DynamicProtoUtils.validateProtoSchema(protoTransportPayloadTypeConfiguration.getDeviceRpcResponseProtoSchema(), RPC_RESPONSE_PROTO_SCHEMA, EXCEPTION_PREFIX);
        } catch (Exception exception) {
            throw new DataValidationException(exception.getMessage());
        }
    }


    private void validateTelemetryDynamicMessageFields(ProtoTransportPayloadConfiguration protoTransportPayloadTypeConfiguration) {
        String deviceTelemetryProtoSchema = protoTransportPayloadTypeConfiguration.getDeviceTelemetryProtoSchema();
        Descriptors.Descriptor telemetryDynamicMessageDescriptor = protoTransportPayloadTypeConfiguration.getTelemetryDynamicMessageDescriptor(deviceTelemetryProtoSchema);
        if (telemetryDynamicMessageDescriptor == null) {
            throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(TELEMETRY_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Failed to get telemetryDynamicMessageDescriptor!");
        } else {
            List<Descriptors.FieldDescriptor> fields = telemetryDynamicMessageDescriptor.getFields();
            if (CollectionUtils.isEmpty(fields)) {
                throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(TELEMETRY_PROTO_SCHEMA, EXCEPTION_PREFIX) + " " + telemetryDynamicMessageDescriptor.getName() + " fields is empty!");
            } else if (fields.size() == 2) {
                Descriptors.FieldDescriptor tsFieldDescriptor = telemetryDynamicMessageDescriptor.findFieldByName("ts");
                Descriptors.FieldDescriptor valuesFieldDescriptor = telemetryDynamicMessageDescriptor.findFieldByName("values");
                if (tsFieldDescriptor != null && valuesFieldDescriptor != null) {
                    if (!Descriptors.FieldDescriptor.Type.MESSAGE.equals(valuesFieldDescriptor.getType())) {
                        throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(TELEMETRY_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Field 'values' has invalid data type. Only message type is supported!");
                    }
                    if (!Descriptors.FieldDescriptor.Type.INT64.equals(tsFieldDescriptor.getType())) {
                        throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(TELEMETRY_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Field 'ts' has invalid data type. Only int64 type is supported!");
                    }
                    if (!tsFieldDescriptor.hasOptionalKeyword()) {
                        throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(TELEMETRY_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Field 'ts' has invalid label. Field 'ts' should have optional keyword!");
                    }
                }
            }
        }
    }

    private void validateRpcRequestDynamicMessageFields(ProtoTransportPayloadConfiguration protoTransportPayloadTypeConfiguration) {
        DynamicMessage.Builder rpcRequestDynamicMessageBuilder = protoTransportPayloadTypeConfiguration.getRpcRequestDynamicMessageBuilder(protoTransportPayloadTypeConfiguration.getDeviceRpcRequestProtoSchema());
        Descriptors.Descriptor rpcRequestDynamicMessageDescriptor = rpcRequestDynamicMessageBuilder.getDescriptorForType();
        if (rpcRequestDynamicMessageDescriptor == null) {
            throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(RPC_REQUEST_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Failed to get rpcRequestDynamicMessageDescriptor!");
        } else {
            if (CollectionUtils.isEmpty(rpcRequestDynamicMessageDescriptor.getFields()) || rpcRequestDynamicMessageDescriptor.getFields().size() != 3) {
                throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(RPC_REQUEST_PROTO_SCHEMA, EXCEPTION_PREFIX) + " " + rpcRequestDynamicMessageDescriptor.getName() + " message should always contains 3 fields: method, requestId and params!");
            }
            Descriptors.FieldDescriptor methodFieldDescriptor = rpcRequestDynamicMessageDescriptor.findFieldByName("method");
            if (methodFieldDescriptor == null) {
                throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(RPC_REQUEST_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Failed to get field descriptor for field: method!");
            } else {
                if (!Descriptors.FieldDescriptor.Type.STRING.equals(methodFieldDescriptor.getType())) {
                    throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(RPC_REQUEST_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Field 'method' has invalid data type. Only string type is supported!");
                }
                if (methodFieldDescriptor.isRepeated()) {
                    throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(RPC_REQUEST_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Field 'method' has invalid label!");
                }
            }
            Descriptors.FieldDescriptor requestIdFieldDescriptor = rpcRequestDynamicMessageDescriptor.findFieldByName("requestId");
            if (requestIdFieldDescriptor == null) {
                throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(RPC_REQUEST_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Failed to get field descriptor for field: requestId!");
            } else {
                if (!Descriptors.FieldDescriptor.Type.INT32.equals(requestIdFieldDescriptor.getType())) {
                    throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(RPC_REQUEST_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Field 'requestId' has invalid data type. Only int32 type is supported!");
                }
                if (requestIdFieldDescriptor.isRepeated()) {
                    throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(RPC_REQUEST_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Field 'requestId' has invalid label!");
                }
            }
            Descriptors.FieldDescriptor paramsFieldDescriptor = rpcRequestDynamicMessageDescriptor.findFieldByName("params");
            if (paramsFieldDescriptor == null) {
                throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(RPC_REQUEST_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Failed to get field descriptor for field: params!");
            } else {
                if (paramsFieldDescriptor.isRepeated()) {
                    throw new DataValidationException(DynamicProtoUtils.invalidSchemaProvidedMessage(RPC_REQUEST_PROTO_SCHEMA, EXCEPTION_PREFIX) + " Field 'params' has invalid label!");
                }
            }
        }
    }

    private boolean isDeviceProfileCertificateInJavaCacerts(String deviceProfileX509Secret) {
        try {
            FileInputStream is = new FileInputStream(javaCacertsPath);
            KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
            keystore.load(is, javaCacertsPassword.toCharArray());

            PKIXParameters params = new PKIXParameters(keystore);
            for (TrustAnchor ta : params.getTrustAnchors()) {
                X509Certificate cert = ta.getTrustedCert();
                if (getCertificateString(cert).equals(deviceProfileX509Secret)) {
                    return true;
                }
            }
        } catch (Exception e) {
            log.trace("Failed to validate certificate due to: ", e);
        }
        return false;
    }

    private String getCertificateString(X509Certificate cert) throws CertificateEncodingException {
//        return EncryptionUtil.certTrimNewLines(Base64Utils.encodeToString(cert.getEncoded()));
        return EncryptionUtil.certTrimNewLines(Base64.getEncoder().encodeToString(cert.getEncoded()));
    }
}
