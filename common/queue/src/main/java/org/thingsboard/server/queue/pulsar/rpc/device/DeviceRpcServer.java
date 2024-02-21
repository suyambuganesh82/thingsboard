package org.thingsboard.server.queue.pulsar.rpc.device;

import io.triveni.platform.rpc.device.ValidateMqttAuthCommand;
import io.triveni.platform.rpc.device.ValidateMqttAuthEvent;
import io.triveni.platform.rpc.server.RpcServer;
import io.triveni.platform.rpc.server.RpcServerConfiguration;
import jakarta.annotation.PostConstruct;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarTopic;
import org.springframework.stereotype.Service;
import org.thingsboard.server.queue.pulsar.config.PulsarConfig;
import org.thingsboard.server.rpc.DeviceRpcService;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Slf4j
@Service
public class DeviceRpcServer {

    private final PulsarConfig pulsarConfig;
    private final PulsarAdministration pulsarAdministration;
    private final PulsarClient pulsarClient;
    private final DeviceRpcService deviceRpcService;
    RpcServer rpcServer;

    public DeviceRpcServer(PulsarConfig pulsarConfig, PulsarAdministration pulsarAdministration,
                           PulsarClient pulsarClient, DeviceRpcService deviceRpcService) {
        this.pulsarConfig = pulsarConfig;
        this.pulsarAdministration = pulsarAdministration;
        this.pulsarClient = pulsarClient;
        this.deviceRpcService = deviceRpcService;
    }

    @SneakyThrows
    @PostConstruct
    public void init() {
        PulsarTopic requestTopic = PulsarTopic.builder(pulsarConfig.getRpcDeviceRequestTopic()).build();
        PulsarTopic responseTopic = PulsarTopic.builder(pulsarConfig.getRpcDeviceResponseTopic()).build();
        pulsarAdministration.createOrModifyTopics(requestTopic, responseTopic);

        Schema<ValidateMqttAuthCommand> requestSchema = Schema.JSON(ValidateMqttAuthCommand.class);
        Schema<ValidateMqttAuthEvent> responseSchema = Schema.JSON(ValidateMqttAuthEvent.class);

        RpcServerConfiguration<ValidateMqttAuthCommand, ValidateMqttAuthEvent> rpcServerConfiguration = RpcServerConfiguration
                .builder(requestSchema, responseSchema)
                .requestTopicsPattern(pulsarConfig.getRpcDeviceRequestTopic())
                .subscription(pulsarConfig.getRpcDeviceResponseSubscription())
                .channelDiscoveryInterval(Duration.ofSeconds(2))
                .responseTimeout(Duration.ofSeconds(10))
                .build();

        Function<ValidateMqttAuthCommand, CompletableFuture<ValidateMqttAuthEvent>> function = this::authenticate;
        rpcServer = RpcServer.create(pulsarClient, this::authenticate, rpcServerConfiguration);
        log.info("Pulsar RPC service started");
    }

    public CompletableFuture<ValidateMqttAuthEvent> authenticate(ValidateMqttAuthCommand validateMqttAuthCommand) {
        log.info("Received ValidateMqttAuthCommand from RPC Client: {}", validateMqttAuthCommand);
        ValidateMqttAuthEvent validateMqttAuthEvent = deviceRpcService.process(validateMqttAuthCommand);
        log.info("Processed ValidateMqttAuthCommand and Event is: {}", validateMqttAuthEvent);
        return CompletableFuture.completedFuture(validateMqttAuthEvent);
    }
}
