/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.mqttsnet.thinglinks;

import cn.hutool.core.text.CharSequenceUtil;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.distservice.DistError;
import com.baidu.bifromq.plugin.eventcollector.distservice.Disted;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.PingReq;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientconnected.ClientConnected;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByServer;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Kicked;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.subhandling.SubAcked;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.subhandling.UnsubAcked;
import com.baidu.bifromq.type.ClientInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.mqttsnet.thinglinks.config.EventCollectorConfig;
import com.mqttsnet.thinglinks.config.PluginConfig;
import com.mqttsnet.thinglinks.util.TaskQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pf4j.Extension;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * -----------------------------------------------------------------------------
 * File Name: BifromqEventCollectorPluginEventProvider
 * -----------------------------------------------------------------------------
 * Description:
 * <a href="https://bifromq.io/zh-Hans/docs/plugin/event_collector/">...</a>
 * 事件收集器
 * <p>
 * 1. 实现IEventCollector接口
 * 2. 通过@Extension注解标记为插件
 * 3. 实现report方法，将事件发送到Kafka
 * 4. 实现close方法，关闭资源
 * -----------------------------------------------------------------------------
 *
 * @author xiaonannet
 * @version 1.0
 * -----------------------------------------------------------------------------
 * Revision History:
 * Date         Author          Version     Description
 * --------      --------     -------   --------------------
 * 2024/2/23       xiaonannet        1.0        Initial creation
 * -----------------------------------------------------------------------------
 * @email
 * @date 2024/2/23 15:36
 */
@Slf4j
@Extension
public final class BifromqEventCollectorPluginEventProvider implements IEventCollector {

    private final KafkaProducer<String, String> producer;

    private final TaskQueue taskQueue = new TaskQueue(8, 100, 60L, TimeUnit.SECONDS);


    private static final Map<EventType, String> TOPIC_MAP = new EnumMap<>(EventType.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        TOPIC_MAP.put(EventType.CLIENT_CONNECTED, "mqtt.client.connected.topic");
        TOPIC_MAP.put(EventType.SUB_ACKED, "mqtt.subscription.acked.topic");
        TOPIC_MAP.put(EventType.UNSUB_ACKED, "mqtt.unsubscription.acked.topic");
        TOPIC_MAP.put(EventType.DISTED, "mqtt.distribution.completed.topic");
        TOPIC_MAP.put(EventType.DIST_ERROR, "mqtt.distribution.error.topic");
        TOPIC_MAP.put(EventType.BY_CLIENT, "mqtt.client.disconnect.topic");
        TOPIC_MAP.put(EventType.BY_SERVER, "mqtt.server.disconnect.topic");
        TOPIC_MAP.put(EventType.KICKED, "mqtt.device.kicked.topic");
        TOPIC_MAP.put(EventType.PING_REQ, "mqtt.ping.req.topic");
    }

    public BifromqEventCollectorPluginEventProvider(BifromqEventCollectorContext context) {
        // 通过 context 获取配置
        PluginConfig pluginConfig = context.getPluginConfig();
        EventCollectorConfig eventCollectorConfig = pluginConfig.getEventCollectorConfig();
        log.info("EventProvider initialized with Kafka server: {}", eventCollectorConfig.getKafkaBootstrapServer());

        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, eventCollectorConfig.getKafkaBootstrapServer());
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProperties.put(ProducerConfig.RETRIES_CONFIG, 3); // 添加重试配置以增强鲁棒性
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "all"); // 确保所有副本都确认

        this.producer = new KafkaProducer<>(kafkaProperties);
    }

    @Override
    public void report(Event<?> eventObj) {

        Event<?> event = (Event<?>) eventObj.clone();
        log.info("Received event: {}", event);

        taskQueue.addTask(() -> {

            if (!TOPIC_MAP.containsKey(event.type())) {
                log.warn("Discarding events of type {} as no mapping exists in TOPIC_MAP.", event.type());
                return;
            }

            switch (event.type()) {
                // 客户端已成功连接到服务器
                case CLIENT_CONNECTED -> executeEvent(event, this::handleClientConnectedEvent);

                // 订阅成功
                case SUB_ACKED -> executeEvent(event, this::handleSubAckedEvent);

                // 取消订阅
                case UNSUB_ACKED -> executeEvent(event, this::handleUnsubAckedEvent);

                // 客户端的遗嘱消息已被分发
                case DISTED -> executeEvent(event, this::handleDistedEvent);

                // 消息分发错误
                case DIST_ERROR -> executeEvent(event, this::handleDistErrorEvent);

                // 客户端主动发送了DISCONNECT消息，断开连接
                case BY_CLIENT -> executeEvent(event, this::handleByClientEvent);

                // 服务器由于某些原因（如客户端违反协议规定）主动断开了与客户端的连接
                case BY_SERVER -> executeEvent(event, this::handleByServerEvent);

                // 客户端被服务器踢下线，可能是因为另一个同样标识符的客户端连接到了服务器
                case KICKED -> executeEvent(event, this::handleKickedEvent);

                // 客户端发送了PING REQ消息
                case PING_REQ -> executeEvent(event, this::handlePingReqEvent);
                default -> log.warn("Discarding events of type {} as no handler exists.", event.type());
            }
        });
    }

    private void executeEvent(Event<?> event, Consumer<Event<?>> handler) {
        // 记录开始执行事件的日志，包括事件类型和当前时间戳（或其他相关信息）
        long startTime = System.currentTimeMillis();
        log.info("Starting execution of event: '{}'. Time: {}", event.type(), startTime);

        // 执行事件处理器
        handler.accept(event);

        // 记录事件执行完成的日志，包括事件类型和耗时
        long endTime = System.currentTimeMillis();
        log.info("Completed execution of event: '{}'. Duration: {} ms", event.type(), endTime - startTime);
    }

    private void createMessageDetailsJson(Event<?> event, Map<String, Object> details) {
        log.info("Processing event type: {}", event.type());

        // 使用computeIfAbsent来确保timestamp存在，而不是先检查再放入
        details.computeIfAbsent("timestamp", key -> System.currentTimeMillis());

        try {
            String messageDetailsJson = objectMapper.writeValueAsString(details);
            sendEventToKafka(TOPIC_MAP.get(event.type()), messageDetailsJson);
            log.info("Sent to Kafka topic {}: {}", TOPIC_MAP.get(event.type()), messageDetailsJson);
        } catch (JsonProcessingException e) {
            log.error("Error serializing message details for event type {}", event.type(), e);
        }

        log.info("Processed event type: {}", event.type());
    }


    /**
     * 处理设备连接事件
     */
    private void handleClientConnectedEvent(Event<?> event) {
        Optional.ofNullable(event)
                .map(e -> (ClientConnected) e.clone())
                .ifPresent(clientConnected -> {
                    Optional<String> tenantIdOpt = Optional.of(clientConnected.clientInfo().getTenantId());
                    Optional<Map<String, String>> metadataMapOpt = Optional.of(clientConnected.clientInfo().getMetadataMap());

                    metadataMapOpt.ifPresent(metadataMap -> {
                        int keepAliveTimeSeconds = clientConnected.keepAliveTimeSeconds();

                        Map<String, Object> messageDetails = new HashMap<>();
                        tenantIdOpt.ifPresent(tenantId -> messageDetails.put("tenantId", tenantId));
                        messageDetails.put("clientId", metadataMap.getOrDefault("clientId", ""));
                        messageDetails.put("success", "success");
                        messageDetails.put("event", "connect");
                        messageDetails.put("version", metadataMap.getOrDefault("ver", ""));
                        messageDetails.put("userId", metadataMap.getOrDefault("userId", ""));
                        messageDetails.put("address", metadataMap.getOrDefault("address", ""));
                        messageDetails.put("channelId", metadataMap.getOrDefault("channelId", ""));
                        messageDetails.put("keepAliveTimeSeconds", keepAliveTimeSeconds);

                        createMessageDetailsJson(clientConnected, messageDetails);
                    });
                });
    }


    /**
     * 订阅成功
     */
    private void handleSubAckedEvent(Event<?> event) {
        Optional.ofNullable(event)
                .map(e -> (SubAcked) e.clone())
                .ifPresent(subAcked -> {
                    Optional<String> tenantIdOpt = Optional.of(subAcked.clientInfo().getTenantId());
                    Optional<Map<String, String>> metadataMapOpt = Optional.of(subAcked.clientInfo().getMetadataMap());

                    metadataMapOpt.ifPresent(metadataMap -> {
                        Map<String, Object> messageDetails = new HashMap<>();
                        tenantIdOpt.ifPresent(tenantId -> messageDetails.put("tenantId", tenantId));
                        messageDetails.put("clientId", metadataMap.getOrDefault("clientId", ""));
                        messageDetails.put("messageId", subAcked.messageId());
                        Optional.ofNullable(subAcked.topicFilter())
                                .filter(topics -> !topics.isEmpty())
                                .map(topics -> topics.get(0))
                                .ifPresent(topic -> messageDetails.put("topic", topic));
                        messageDetails.put("success", "success");
                        messageDetails.put("event", "SUBSCRIBE");
                        messageDetails.put("address", metadataMap.getOrDefault("address", ""));

                        createMessageDetailsJson(subAcked, messageDetails);
                    });
                });
    }


    /**
     * 取消订阅
     */
    private void handleUnsubAckedEvent(Event<?> event) {
        Optional.ofNullable(event)
                .map(e -> (UnsubAcked) e.clone())
                .ifPresent(unsubAcked -> {
                    Optional<String> tenantIdOpt = Optional.of(unsubAcked.clientInfo().getTenantId());
                    Optional<Map<String, String>> metadataMapOpt = Optional.of(unsubAcked.clientInfo().getMetadataMap());

                    metadataMapOpt.ifPresent(metadataMap -> {
                        Map<String, Object> messageDetails = new HashMap<>();
                        tenantIdOpt.ifPresent(tenantId -> messageDetails.put("tenantId", tenantId));
                        messageDetails.put("clientId", metadataMap.getOrDefault("clientId", ""));
                        messageDetails.put("messageId", unsubAcked.messageId());
                        Optional.ofNullable(unsubAcked.topicFilter())
                                .filter(topics -> !topics.isEmpty())
                                .map(topics -> topics.get(0))
                                .ifPresent(topic -> messageDetails.put("topic", topic));
                        messageDetails.put("success", "success");
                        messageDetails.put("event", "UNSUBSCRIBE");
                        messageDetails.put("address", metadataMap.getOrDefault("address", ""));

                        createMessageDetailsJson(unsubAcked, messageDetails);
                    });
                });
    }


    /**
     * 客户端的遗嘱消息已被分发
     */
    private void handleDistedEvent(Event<?> event) {
        Optional.ofNullable(event)
                .map(e -> (Disted) e.clone())
                .ifPresent(disted -> {
                    Optional.ofNullable(disted.messages())
                            .ifPresent(messagePack -> {
                                messagePack.forEach(pack -> {
                                    Optional<String> tenantIdOpt = Optional.ofNullable(pack.getPublisher())
                                            .map(ClientInfo::getTenantId);
                                    Optional.of(pack.getMessagePackList())
                                            .ifPresent(messagePackList -> {
                                                messagePackList.parallelStream().forEach(msg -> {
                                                    String topic = msg.getTopic();
                                                    Optional.of(msg.getMessageList())
                                                            .ifPresent(messageList -> {
                                                                messageList.parallelStream().forEach(message -> {
                                                                    long messageId = message.getMessageId();
                                                                    int pubQoSValue = message.getPubQoSValue();
                                                                    ByteString payload = message.getPayload();
                                                                    long timestamp = message.getTimestamp();
                                                                    long expiryInterval = message.getExpiryInterval();
                                                                    String payloadStr = payload.toStringUtf8();

                                                                    Map<String, Object> messageDetails = new HashMap<>();
                                                                    tenantIdOpt.ifPresent(tenantId -> messageDetails.put("tenantId", tenantId));
                                                                    messageDetails.put("topic", topic);
                                                                    messageDetails.put("messageId", messageId);
                                                                    messageDetails.put("qos", pubQoSValue);
                                                                    messageDetails.put("timestamp", timestamp);
                                                                    messageDetails.put("event", "PUBLISH");
                                                                    messageDetails.put("time", timestamp);
                                                                    messageDetails.put("expiryInterval", expiryInterval);
                                                                    messageDetails.put("payload", payloadStr);
                                                                    messageDetails.put("body", payloadStr);

                                                                    createMessageDetailsJson(disted, messageDetails);
                                                                });
                                                            });
                                                });
                                            });
                                });
                            });
                });
    }


    /**
     * 消息分发错误
     */
    private void handleDistErrorEvent(Event<?> event) {
        Optional.ofNullable(event)
                .map(e -> (DistError) e.clone())
                .ifPresent(distError -> {
                    String tenantId = "";  // This doesn't seem to change or get assigned a value based on the given code

                    Map<String, Object> messageDetails = new HashMap<>();
                    messageDetails.put("clientId", distError.reqId());
                    messageDetails.put("tenantId", tenantId);
                    Optional.ofNullable(distError.messages())
                            .ifPresent(messages -> messageDetails.put("message", messages.toString()));
                    messageDetails.put("success", "success");
                    messageDetails.put("event", "error");
                    messageDetails.put("reqId", distError.reqId());
                    messageDetails.put("code", distError.code());

                    createMessageDetailsJson(distError, messageDetails);
                });
    }


    /**
     * 客户端主动发送了DISCONNECT消息，断开连接。
     */
    private void handleByClientEvent(Event<?> event) {
        Optional.ofNullable(event)
                .map(e -> (ByClient) e.clone())
                .ifPresent(byClient -> {
                    Map<String, Object> messageDetails = new HashMap<>();
                    messageDetails.put("tenantId", byClient.clientInfo().getTenantId());

                    Optional.of(byClient.clientInfo().getMetadataMap())
                            .ifPresent(metadata -> {
                                messageDetails.put("clientId", metadata.get("clientId"));
                                messageDetails.put("address", metadata.get("address"));
                            });

                    messageDetails.put("success", "success");
                    messageDetails.put("event", "DISCONNECT");

                    createMessageDetailsJson(byClient, messageDetails);
                });
    }


    /**
     * 服务器由于某些原因（如客户端违反协议规定）主动断开了与客户端的连接。
     */
    private void handleByServerEvent(Event<?> event) {
        Optional.ofNullable(event)
                .map(e -> (ByServer) e.clone())
                .ifPresent(byServer -> {
                    Map<String, Object> messageDetails = new HashMap<>();
                    messageDetails.put("tenantId", byServer.clientInfo().getTenantId());

                    Optional.of(byServer.clientInfo().getMetadataMap())
                            .ifPresent(metadata -> {
                                messageDetails.put("clientId", metadata.get("clientId"));
                                messageDetails.put("address", metadata.get("address"));
                            });

                    messageDetails.put("success", "success");
                    messageDetails.put("event", "CLOSE");

                    createMessageDetailsJson(byServer, messageDetails);
                });
    }


    /**
     * 客户端被服务器踢下线，可能是因为另一个同样标识符的客户端连接到了服务器。
     */
    private void handleKickedEvent(Event<?> event) {
        Optional.ofNullable(event)
                .map(e -> (Kicked) e.clone())
                .ifPresent(kicked -> {
                    Map<String, Object> messageDetails = new HashMap<>();
                    messageDetails.put("tenantId", kicked.clientInfo().getTenantId());

                    Optional.of(kicked.clientInfo().getMetadataMap())
                            .ifPresent(metadata -> {
                                messageDetails.put("clientId", metadata.get("clientId"));
                                messageDetails.put("address", metadata.get("address"));
                            });

                    messageDetails.put("success", "success");
                    messageDetails.put("event", "CLOSE");

                    createMessageDetailsJson(kicked, messageDetails);
                });
    }


    /**
     * 客户端发送了PING REQ消息
     */
    private void handlePingReqEvent(Event<?> event) {
        Optional.ofNullable(event)
                .map(e -> (PingReq) e.clone())
                .ifPresent(pingReq -> {
                    Map<String, Object> messageDetails = new HashMap<>();
                    messageDetails.put("tenantId", pingReq.clientInfo().getTenantId());

                    Optional.of(pingReq.clientInfo().getMetadataMap())
                            .ifPresent(metadata -> {
                                messageDetails.put("clientId", metadata.getOrDefault("clientId", ""));
                                messageDetails.put("version", metadata.getOrDefault("ver", ""));
                                messageDetails.put("userId", metadata.getOrDefault("userId", ""));
                                messageDetails.put("address", metadata.getOrDefault("address", ""));
                                messageDetails.put("channelId", metadata.getOrDefault("channelId", ""));
                            });

                    messageDetails.put("success", "success");
                    messageDetails.put("event", "PING");

                    createMessageDetailsJson(pingReq, messageDetails);
                });
    }


    /**
     * 消息生产
     */
    private void sendEventToKafka(String topic, String message) {

        if (CharSequenceUtil.isBlank(topic) || CharSequenceUtil.isBlank(message)) {
            log.warn("Cannot send null event to Kafka.");
            return;
        }

        producer.send(new ProducerRecord<>(topic, message), (recordMetadata, e) -> {
            if (e != null) {
                log.error("Error occurred while producing message to topic {}. Exception: ", recordMetadata.topic(), e);
            } else {
                log.info("Message successfully sent to topic {}, offset:{}.", recordMetadata.topic(), recordMetadata.offset());
            }
            log.info("topic:{},end", topic);
        });
    }

    @Override
    public void close() {
        taskQueue.shutdown();
        producer.close();
    }

}
