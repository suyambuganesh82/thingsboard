package org.thingsboard.server.rpc;

import io.triveni.platform.rpc.device.ValidateMqttAuthCommand;
import io.triveni.platform.rpc.device.ValidateMqttAuthEvent;

public interface DeviceRpcService {

    ValidateMqttAuthEvent process(ValidateMqttAuthCommand validateMqttAuthCommand);
}
