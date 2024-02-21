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
package org.thingsboard.server.service.pulsar.queue.msg;

import io.triveni.broker.mqtt.message.MqttClientEvent;
import lombok.Data;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.MsgType;
import org.thingsboard.server.common.msg.TbActorMsg;
import org.thingsboard.server.common.msg.aware.DeviceAwareMsg;
import org.thingsboard.server.common.msg.aware.TenantAwareMsg;
import org.thingsboard.server.common.msg.queue.TbCallback;

import java.io.Serializable;

@Data
public class MqttClientEventToDeviceActorMsg implements TbActorMsg, DeviceAwareMsg, TenantAwareMsg, Serializable {

    private static final long serialVersionUID = 7191333353202935941L;

    private final TenantId tenantId;
    private final DeviceId deviceId;
    private final MqttClientEvent msg;
    private final TbCallback callback;

    public MqttClientEventToDeviceActorMsg(MqttClientEvent msg, TbCallback callback) {
        this.msg = msg;
        this.callback = callback;
        this.tenantId = TenantId.fromUUID(msg.mqttSessionInfo().tenantId().get());
        this.deviceId = new DeviceId(msg.mqttSessionInfo().deviceId().get());
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.MQTT_CLIENT_EVENT_TO_DEVICE_ACTOR;
    }
}
