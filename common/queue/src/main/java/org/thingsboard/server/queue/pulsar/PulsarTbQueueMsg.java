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
package org.thingsboard.server.queue.pulsar;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.pulsar.client.api.Message;
import org.thingsboard.server.queue.TbQueueMsg;
import org.thingsboard.server.queue.TbQueueMsgHeaders;
import org.thingsboard.server.queue.common.DefaultTbQueueMsgHeaders;

import java.util.Map;
import java.util.UUID;

public class PulsarTbQueueMsg implements TbQueueMsg {
    private final UUID key;
    private final TbQueueMsgHeaders headers;
    private final byte[] data;

    public PulsarTbQueueMsg(Message<byte[]> record) {
        this.key = UUID.fromString(record.getKey());
        TbQueueMsgHeaders headers = new DefaultTbQueueMsgHeaders();
        for (Map.Entry<String, String> entry : record.getProperties().entrySet()) {
            headers.put(String.valueOf(entry.getKey()), StringUtils.getBytesUtf8(entry.getValue()));
        }
        this.headers = headers;
        this.data = record.getValue();
    }

    @Override
    public UUID getKey() {
        return key;
    }

    @Override
    public TbQueueMsgHeaders getHeaders() {
        return headers;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
