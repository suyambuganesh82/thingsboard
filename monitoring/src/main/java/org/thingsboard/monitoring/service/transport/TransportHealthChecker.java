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
package org.thingsboard.monitoring.service.transport;

import com.fasterxml.jackson.databind.node.TextNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.monitoring.client.TbClient;
import org.thingsboard.monitoring.config.transport.*;
import org.thingsboard.monitoring.service.BaseHealthChecker;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceProfileType;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.device.data.DefaultDeviceConfiguration;
import org.thingsboard.server.common.data.device.data.DefaultDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.DeviceData;
import org.thingsboard.server.common.data.device.profile.DefaultDeviceProfileConfiguration;
import org.thingsboard.server.common.data.device.profile.DefaultDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.DeviceProfileData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.DeviceCredentialsType;

@Slf4j
public abstract class TransportHealthChecker<C extends TransportMonitoringConfig> extends BaseHealthChecker<C, TransportMonitoringTarget> {

    public TransportHealthChecker(C config, TransportMonitoringTarget target) {
        super(config, target);
    }

    @Override
    protected void initialize(TbClient tbClient) {
        Device device = getOrCreateDevice(tbClient);
        DeviceCredentials credentials = tbClient.getDeviceCredentialsByDeviceId(device.getId())
                .orElseThrow(() -> new IllegalArgumentException("No credentials found for device " + device.getId()));

        DeviceConfig deviceConfig = new DeviceConfig();
        deviceConfig.setId(device.getId().toString());
        deviceConfig.setName(device.getName());
        deviceConfig.setCredentials(credentials);
        target.setDevice(deviceConfig);
    }

    @Override
    protected String createTestPayload(String testValue) {
        return JacksonUtil.newObjectNode().set(TEST_TELEMETRY_KEY, new TextNode(testValue)).toString();
    }

    @Override
    protected Object getInfo() {
        return new TransportInfo(getTransportType(), target.getBaseUrl(), target.getQueue());
    }

    @Override
    protected String getKey() {
        return getTransportType().name().toLowerCase() + (target.getQueue().equals("Main") ? "" : target.getQueue()) + "Transport";
    }

    protected abstract TransportType getTransportType();


    private Device getOrCreateDevice(TbClient tbClient) {
        TransportType transportType = config.getTransportType();
        String deviceName = String.format("%s (%s) - %s", transportType.getName(), target.getQueue(), target.getBaseUrl());
        Device device = tbClient.getTenantDevice(deviceName).orElse(null);
        if (device != null) {
            return device;
        }

        log.info("Creating new device '{}'", deviceName);
        device = new Device();
        device.setName(deviceName);

        DeviceCredentials credentials = new DeviceCredentials();
        credentials.setCredentialsId(RandomStringUtils.randomAlphabetic(20));
        DeviceData deviceData = new DeviceData();
        deviceData.setConfiguration(new DefaultDeviceConfiguration());

        DeviceProfile deviceProfile = getOrCreateDeviceProfile(tbClient);
        device.setType(deviceProfile.getName());
        device.setDeviceProfileId(deviceProfile.getId());

        deviceData.setTransportConfiguration(new DefaultDeviceTransportConfiguration());
        credentials.setCredentialsType(DeviceCredentialsType.ACCESS_TOKEN);
        return tbClient.saveDeviceWithCredentials(device, credentials).get();
    }

    private DeviceProfile getOrCreateDeviceProfile(TbClient tbClient) {
        TransportType transportType = config.getTransportType();
        String profileName = String.format("%s (%s)", transportType.getName(), target.getQueue());
        DeviceProfile deviceProfile = tbClient.getDeviceProfiles(new PageLink(1, 0, profileName)).getData()
                .stream().findFirst().orElse(null);
        if (deviceProfile != null) {
            return deviceProfile;
        }

        log.info("Creating new device profile '{}'", profileName);
            deviceProfile = new DeviceProfile();
            deviceProfile.setType(DeviceProfileType.DEFAULT);
            deviceProfile.setTransportType(DeviceTransportType.DEFAULT);
            DeviceProfileData profileData = new DeviceProfileData();
            profileData.setConfiguration(new DefaultDeviceProfileConfiguration());
            profileData.setTransportConfiguration(new DefaultDeviceProfileTransportConfiguration());
            deviceProfile.setProfileData(profileData);
        deviceProfile.setName(profileName);
        deviceProfile.setDefaultQueueName(target.getQueue());
        return tbClient.saveDeviceProfile(deviceProfile);
    }

}
