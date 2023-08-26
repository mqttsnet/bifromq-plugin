package bifromq.plugin.provider;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import bifromq.plugin.config.ConfigUtil;
import bifromq.plugin.utils.TaskQueue;
import cn.hutool.core.text.CharSequenceUtil;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.distservice.DistError;
import com.baidu.bifromq.plugin.eventcollector.distservice.Disted;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientconnected.ClientConnected;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByServer;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Kicked;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.subhandling.SubAcked;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.subhandling.UnsubAcked;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pf4j.Extension;

@Extension
@Slf4j
public final class EventKafkaProvider implements IEventCollector {

    private final KafkaProducer<String, String> producer;

    private final TaskQueue taskQueue = new TaskQueue(4, 10, 60L, TimeUnit.SECONDS);


    private static final Map<EventType, String> TOPIC_MAP = new EnumMap<>(EventType.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        TOPIC_MAP.put(EventType.CLIENT_CONNECTED, "client.connected.topic");
        TOPIC_MAP.put(EventType.SUB_ACKED, "subscription.acked.topic");
        TOPIC_MAP.put(EventType.UNSUB_ACKED, "unsubscription.acked.topic");
        TOPIC_MAP.put(EventType.DISTED, "distribution.completed.topic");
        TOPIC_MAP.put(EventType.DIST_ERROR, "distribution.error.topic");
        TOPIC_MAP.put(EventType.BY_CLIENT, "client.disconnect.topic");
        TOPIC_MAP.put(EventType.BY_SERVER, "server.disconnect.topic");
        TOPIC_MAP.put(EventType.KICKED, "device.kicked.topic");
    }

    public EventKafkaProvider() {

        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConfigUtil.getPluginConfig().getEventCollectorConfig().getKafkaBootstrapServer());
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.producer = new KafkaProducer<>(kafkaProperties);
    }

    @Override
    public void report(Event<?> eventObj) {

        Event<?> event = (Event<?>) eventObj.clone();

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
                default -> log.warn("Discarding events of type {} as no handler exists.", event.type());
            }
        });
    }

    private void executeEvent(Event<?> event, Consumer<Event<?>> handler) {
        log.info("executeEvent:{} start", event.type());
        handler.accept(event);
        log.info("executeEvent:{} end", event.type());
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
                        messageDetails.put("address", metadataMap.getOrDefault("address", ""));
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
                    String tenantId = "";  // This doesn't seem to change or get assigned a value based on the given code

                    Optional.ofNullable(disted.messages())
                            .ifPresent(messagePack -> {
                                messagePack.forEach(pack -> {
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
                                                                    long expireTimestamp = message.getExpireTimestamp();
                                                                    String payloadStr = payload.toStringUtf8();

                                                                    Map<String, Object> messageDetails = new HashMap<>();
                                                                    messageDetails.put("topic", topic);
                                                                    messageDetails.put("tenantId", tenantId);
                                                                    messageDetails.put("messageId", messageId);
                                                                    messageDetails.put("qos", pubQoSValue);
                                                                    messageDetails.put("timestamp", timestamp);
                                                                    messageDetails.put("event", "PUBLISH");
                                                                    messageDetails.put("time", timestamp);
                                                                    messageDetails.put("expireTimestamp", expireTimestamp);
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
                log.info("Message successfully sent to topic {}.", recordMetadata.topic());
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
