package bifromq.plugin.provider;

import bifromq.plugin.config.ConfigUtil;
import cn.hutool.core.text.CharSequenceUtil;
import com.alibaba.fastjson2.JSON;
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
import com.baidu.bifromq.type.PublisherMessagePack;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pf4j.Extension;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

@Extension
@Slf4j
public final class EventKafkaProvider implements IEventCollector {
    private static final int CORE_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 10;
    private static final int MAX_POOL_SIZE = CORE_POOL_SIZE * 20;
    private static final long KEEP_ALIVE_TIME = 60L;

    private final KafkaProducer<String, String> producer;
    private final ExecutorService executor;

    private static final Map<EventType, String> TOPIC_MAP = new EnumMap<>(EventType.class);

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
        this.executor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE_TIME,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());

        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConfigUtil.getPluginConfig().getEventCollectorConfig().getKafkaBootstrapServer());
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.producer = new KafkaProducer<>(kafkaProperties);
    }

    @Override
    public void report(Event<?> event) {
        log.info("Received event - eventType: {}, event: {}", event.type(), event);

        if (!TOPIC_MAP.containsKey(event.type())) {
            log.warn("Discarding events of type {} as no mapping exists in TOPIC_MAP.", event.type());
            return;
        }

        switch (event.type()) {
            // 客户端已成功连接到服务器
            case CLIENT_CONNECTED:
                log.info("CLIENT_CONNECTED start");
                executor.execute(() -> handleClientConnectedEvent(event));
                log.info("CLIENT_CONNECTED end");
                break;
            // 订阅成功
            case SUB_ACKED:
                log.info("SUB_ACKED start");
                executor.execute(() -> handleSubAckedEvent(event));
                log.info("SUB_ACKED end");
                break;
            // 取消订阅
            case UNSUB_ACKED:
                log.info("UNSUB_ACKED start");
                executor.execute(() -> handleUnsubAckedEvent(event));
                log.info("UNSUB_ACKED end");
                break;
            // 客户端的遗嘱消息已被分发
            case DISTED:
                log.info("DISTED start");
                executor.execute(() -> handleDistedEvent(event));
                log.info("DISTED end");
                break;
            // 消息分发错误
            case DIST_ERROR:
                log.info("DIST_ERROR start");
                executor.execute(() -> handleDistErrorEvent(event));
                log.info("DIST_ERROR end");
                break;
            // 客户端主动发送了DISCONNECT消息，断开连接
            case BY_CLIENT:
                log.info("BY_CLIENT start");
                executor.execute(() -> handleByClientEvent(event));
                log.info("BY_CLIENT end");
                break;
            // 服务器由于某些原因（如客户端违反协议规定）主动断开了与客户端的连接
            case BY_SERVER:
                log.info("BY_SERVER start");
                executor.execute(() -> handleByServerEvent(event));
                log.info("BY_SERVER end");
                break;
            // 客户端被服务器踢下线，可能是因为另一个同样标识符的客户端连接到了服务器
            case KICKED:
                log.info("KICKED start");
                executor.execute(() -> handleKickedEvent(event));
                log.info("KICKED end");
                break;
            default:
                log.warn("Discarding events of type {} as no handler exists.", event.type());
                break;
        }
    }

    /**
     * 处理设备连接事件
     *
     * @param event
     */
    private void handleClientConnectedEvent(Event<?> event) {

        ClientConnected clientConnected = (ClientConnected) event.clone();
        String tenantId = clientConnected.clientInfo().getTenantId();
        Map<String, String> metadataMap = clientConnected.clientInfo().getMetadataMap();

        if (metadataMap != null) {

            int keepAliveTimeSeconds = clientConnected.keepAliveTimeSeconds();

            ObjectMapper objectMapper = new ObjectMapper();

            Map<String, Object> messageDetails = new HashMap<>();
            messageDetails.put("tenantId", tenantId);
            messageDetails.put("clientId", metadataMap.get("clientId"));
            messageDetails.put("success", "success");
            messageDetails.put("event", event.type());
            messageDetails.put("address", metadataMap.get("address"));
            messageDetails.put("keepAliveTimeSeconds", keepAliveTimeSeconds);
            messageDetails.put("timestamp", System.currentTimeMillis());

            try {
                String messageDetailsJson = objectMapper.writeValueAsString(messageDetails);
                sendEventToKafka(TOPIC_MAP.get(event.type()), messageDetailsJson);
            } catch (JsonProcessingException e) {
                log.error("Error occurred while serializing message details. Exception: ", e);
            }
        }
    }

    /*
     * 订阅成功
     *
     * */
    private void handleSubAckedEvent(Event<?> event) {
        SubAcked subAcked = (SubAcked) event.clone();

        String tenantId = subAcked.clientInfo().getTenantId();
        Map<String, String> metadataMap = subAcked.clientInfo().getMetadataMap();

        if (metadataMap != null) {

            ObjectMapper objectMapper = new ObjectMapper();

            Map<String, Object> messageDetails = new HashMap<>();
            messageDetails.put("tenantId", tenantId);
            messageDetails.put("clientId", metadataMap.get("clientId"));
            messageDetails.put("messageId", subAcked.messageId());
            messageDetails.put("topic", subAcked.topicFilter().get(0));
            messageDetails.put("success", "success");
            messageDetails.put("event", event.type());
            messageDetails.put("address", metadataMap.get("address"));
            messageDetails.put("timestamp", System.currentTimeMillis());

            try {
                String messageDetailsJson = objectMapper.writeValueAsString(messageDetails);
                sendEventToKafka(TOPIC_MAP.get(event.type()), messageDetailsJson);
            } catch (JsonProcessingException e) {
                log.error("Error occurred while serializing message details. Exception: ", e);
            }
        }

    }

    /*
     * 取消订阅
     *
     * */
    private void handleUnsubAckedEvent(Event<?> event) {
        UnsubAcked unsubAcked = (UnsubAcked) event.clone();

        String tenantId = unsubAcked.clientInfo().getTenantId();
        Map<String, String> metadataMap = unsubAcked.clientInfo().getMetadataMap();

        if (metadataMap != null) {

            ObjectMapper objectMapper = new ObjectMapper();

            Map<String, Object> messageDetails = new HashMap<>();
            messageDetails.put("tenantId", tenantId);
            messageDetails.put("clientId", metadataMap.get("clientId"));
            messageDetails.put("messageId", unsubAcked.messageId());
            messageDetails.put("topic", unsubAcked.topicFilter().get(0));
            messageDetails.put("success", "success");
            messageDetails.put("event", event.type());
            messageDetails.put("address", metadataMap.get("address"));
            messageDetails.put("timestamp", System.currentTimeMillis());

            try {
                String messageDetailsJson = objectMapper.writeValueAsString(messageDetails);
                sendEventToKafka(TOPIC_MAP.get(event.type()), messageDetailsJson);
            } catch (JsonProcessingException e) {
                log.error("Error occurred while serializing message details. Exception: ", e);
            }
        }
    }

    /*
     * 客户端的遗嘱消息已被分发
     *
     * */
    private void handleDistedEvent(Event<?> event) {
        Disted disted = (Disted) event.clone();
        ObjectMapper objectMapper = new ObjectMapper();

        Iterable<PublisherMessagePack> messagePack = disted.messages();
        messagePack.forEach(pack -> {
            List<PublisherMessagePack.TopicPack> messagePackList = pack.getMessagePackList();
            messagePackList.parallelStream().forEach(msg -> {
                String topic = msg.getTopic();
                msg.getMessageList().parallelStream().forEach(message -> {
                    long messageId = message.getMessageId();
                    int pubQoSValue = message.getPubQoSValue();
                    ByteString payload = message.getPayload();
                    long timestamp = message.getTimestamp();
                    long expireTimestamp = message.getExpireTimestamp();
                    String payloadStr = payload.toStringUtf8();

                    Map<String, Object> messageDetails = new HashMap<>();
                    messageDetails.put("topic", topic);
                    messageDetails.put("messageId", messageId);
                    messageDetails.put("qos", pubQoSValue);
                    messageDetails.put("timestamp", timestamp);
                    messageDetails.put("time", timestamp);
                    messageDetails.put("expireTimestamp", expireTimestamp);
                    messageDetails.put("payload", payloadStr);
                    messageDetails.put("body", payloadStr);

                    try {
                        String messageDetailsJson = objectMapper.writeValueAsString(messageDetails);
                        sendEventToKafka(TOPIC_MAP.get(event.type()), messageDetailsJson);
                    } catch (JsonProcessingException e) {
                        log.error("Error occurred while serializing message details. Exception: ", e);
                    }
                });
            });
        });
    }

    /*
     * 消息分发错误
     *
     * */
    private void handleDistErrorEvent(Event<?> event) {
        DistError distError = (DistError) event.clone();

        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, Object> messageDetails = new HashMap<>();
        messageDetails.put("clientId", distError.reqId());
        messageDetails.put("message", distError.messages().toString());
        messageDetails.put("success", "success");
        messageDetails.put("event", event.type());
        messageDetails.put("reqId", distError.reqId());
        messageDetails.put("code", distError.code());
        messageDetails.put("timestamp", System.currentTimeMillis());

        try {
            String messageDetailsJson = objectMapper.writeValueAsString(messageDetails);
            sendEventToKafka(TOPIC_MAP.get(event.type()), messageDetailsJson);
        } catch (JsonProcessingException e) {
            log.error("Error occurred while serializing message details. Exception: ", e);
        }

    }

    /*
     * 客户端主动发送了DISCONNECT消息，断开连接
     *
     * */
    private void handleByClientEvent(Event<?> event) {
        ByClient byClient = (ByClient) event.clone();

        String tenantId = byClient.clientInfo().getTenantId();
        Map<String, String> metadataMap = byClient.clientInfo().getMetadataMap();

        if (metadataMap != null) {

            ObjectMapper objectMapper = new ObjectMapper();

            Map<String, Object> messageDetails = new HashMap<>();
            messageDetails.put("tenantId", tenantId);
            messageDetails.put("clientId", metadataMap.get("clientId"));
            messageDetails.put("success", "success");
            messageDetails.put("event", event.type());
            messageDetails.put("address", metadataMap.get("address"));
            messageDetails.put("timestamp", System.currentTimeMillis());

            try {
                String messageDetailsJson = objectMapper.writeValueAsString(messageDetails);
                sendEventToKafka(TOPIC_MAP.get(event.type()), messageDetailsJson);
            } catch (JsonProcessingException e) {
                log.error("Error occurred while serializing message details. Exception: ", e);
            }
        }
    }

    /*
     * 客户端被服务器踢下线，可能是因为另一个同样标识符的客户端连接到了服务器
     *
     * */
    private void handleByServerEvent(Event<?> event) {
        ByServer byServer = (ByServer) event.clone();

        String tenantId = byServer.clientInfo().getTenantId();
        Map<String, String> metadataMap = byServer.clientInfo().getMetadataMap();

        if (metadataMap != null) {

            ObjectMapper objectMapper = new ObjectMapper();

            Map<String, Object> messageDetails = new HashMap<>();
            messageDetails.put("tenantId", tenantId);
            messageDetails.put("clientId", metadataMap.get("clientId"));
            messageDetails.put("success", "success");
            messageDetails.put("event", event.type());
            messageDetails.put("address", metadataMap.get("address"));
            messageDetails.put("timestamp", System.currentTimeMillis());

            try {
                String messageDetailsJson = objectMapper.writeValueAsString(messageDetails);
                sendEventToKafka(TOPIC_MAP.get(event.type()), messageDetailsJson);
            } catch (JsonProcessingException e) {
                log.error("Error occurred while serializing message details. Exception: ", e);
            }
        }
    }

    /*
     *  客户端被服务器踢下线，可能是因为另一个同样标识符的客户端连接到了服务器
     *
     * */
    private void handleKickedEvent(Event<?> event) {
        Kicked kicked = (Kicked) event.clone();

        String tenantId = kicked.clientInfo().getTenantId();
        Map<String, String> metadataMap = kicked.clientInfo().getMetadataMap();

        if (metadataMap != null) {

            ObjectMapper objectMapper = new ObjectMapper();

            Map<String, Object> messageDetails = new HashMap<>();
            messageDetails.put("tenantId", tenantId);
            messageDetails.put("clientId", metadataMap.get("clientId"));
            messageDetails.put("success", "success");
            messageDetails.put("event", event.type());
            messageDetails.put("address", metadataMap.get("address"));
            messageDetails.put("timestamp", System.currentTimeMillis());

            try {
                String messageDetailsJson = objectMapper.writeValueAsString(messageDetails);
                sendEventToKafka(TOPIC_MAP.get(event.type()), messageDetailsJson);
            } catch (JsonProcessingException e) {
                log.error("Error occurred while serializing message details. Exception: ", e);
            }
        }
    }

    /*
    * 消息生产
    *
    * */
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
        });
    }
}
