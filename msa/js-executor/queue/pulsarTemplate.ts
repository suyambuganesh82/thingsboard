///
/// Copyright © 2016-2024 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import config from 'config';
import {_logger} from '../config/logger';
import {JsInvokeMessageProcessor} from '../api/jsInvokeMessageProcessor';
import {IQueue} from './queue.models';

import {Client, Consumer} from 'pulsar-client';

import {exit} from 'process';

export class PulsarTemplate implements IQueue {

    private logger = _logger(`pulsarTemplate`);
    private topicProperties: string = config.get('kafka.topic_properties');

    private pulsarClient: Client;
    private consumer: Consumer;
    private configEntries: any[] = [];

    name = 'Pulsar';

    constructor() {
    }

    async init(): Promise<void> {
        const pulsarBootstrapServers: string = config.get('pulsar.bootstrap.servers');
        const requestTopic: string = config.get('request_topic');

        this.logger.info('Pulsar Bootstrap Servers: %s', pulsarBootstrapServers);
        this.logger.info('Pulsar Requests Topic: %s', requestTopic);

        this.parseTopicProperties();

        this.pulsarClient = new Client({
            serviceUrl: pulsarBootstrapServers,
            operationTimeoutSeconds: 30,
        });

        let partitions = 1;

        for (let i = 0; i < this.configEntries.length; i++) {
            let param = this.configEntries[i];
            if (param.name === 'partitions') {
                partitions = param.value;
                this.configEntries.splice(i, 1);
                break;
            }
        }

        const fullRequestTopic = 'persistent://sangam/thingsboard/' + requestTopic;
        this.logger.info('Pulsar Full Request Topic: %s', fullRequestTopic);

        this.consumer = await this.pulsarClient.subscribe({
            // topic: 'persistent://sangam/thingsboard/my-topic',
            topic: fullRequestTopic,
            subscription: 'js-executor-group',
            subscriptionType: 'Shared',
            ackTimeoutMs: 10000,
        });

        // this.producer = await this.pulsarClient.createProducer({
        //     topic: 'persistent://public/default/my-topic',
        // });

        const messageProcessor = new JsInvokeMessageProcessor(this);

        while (true) {
            let message1 = await this.consumer.receive();
            messageProcessor.onJsInvokeMessage(message1);
            await this.consumer.acknowledge(message1);
        }
    }

    private parseTopicProperties() {
        const props = this.topicProperties.split(';');
        props.forEach(p => {
            const delimiterPosition = p.indexOf(':');
            this.configEntries.push({
                name: p.substring(0, delimiterPosition),
                value: p.substring(delimiterPosition + 1)
            });
        });
    }

    async send(responseTopic: string, msgKey: string, rawResponse: Buffer, headers: any): Promise<any> {
        let data = JSON.stringify(
            {
                key: msgKey,
                data: [...rawResponse],
                headers: headers
            });
        let dataBuffer = Buffer.from(data);

        const fullTopic = 'persistent://sangam/thingsboard/' + responseTopic;
        let producer = await this.pulsarClient.createProducer({
            topic: fullTopic,
        });

        producer.send({data: dataBuffer});
        await producer.flush();
        await producer.close();
    }

    async destroy(): Promise<void> {
        this.logger.info('Stopping Pulsar resources...');

        if (this.consumer) {
            this.logger.info('Stopping Pulsar Consumer...');
            try {
                const _consumer = this.consumer;
                // @ts-ignore
                delete this.consumer;
                await _consumer.close();
                await this.pulsarClient.close();
                this.logger.info('Pulsar Consumer stopped.');
            } catch (e: any) {
                this.logger.info('Pulsar Consumer stop error.');
            }
        }
        this.logger.info('Pulsar resources stopped.');
        exit(0); //same as in version before
    }

}
