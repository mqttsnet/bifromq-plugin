package bifromq.plugin.provider;

import bifromq.plugin.config.ConfigUtil;
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
import com.baidu.bifromq.type.PublisherMessagePack;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

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
            case CLIENT_CONNECTED:
                executor.execute(() -> handleClientConnectedEvent(event));
                break;
            case SUB_ACKED:
                executor.execute(() -> handleSubAckedEvent(event));
                break;
            case UNSUB_ACKED:
                executor.execute(() -> handleUnsubAckedEvent(event));
                break;
            case DISTED:
                executor.execute(() -> handleDistedEvent(event));
                break;
            case DIST_ERROR:
                executor.execute(() -> handleDistErrorEvent(event));
                break;
            case BY_CLIENT:
                executor.execute(() -> handleByClientEvent(event));
                break;
            case BY_SERVER:
                executor.execute(() -> handleByServerEvent(event));
                break;
            case KICKED:
                executor.execute(() -> handleKickedEvent(event));
                break;
            default:
                log.warn("Discarding events of type {} as no handler exists.", event.type());
                break;
        }
    }

    /**
     * 处理设备连接事件
     * @param event
     */
    private void handleClientConnectedEvent(Event<?> event) {
        ClientConnected clientConnected = (ClientConnected) event.clone();
        String tenantId = clientConnected.clientInfo().getTenantId();
        Map<String, String> metadataMap = clientConnected.clientInfo().getMetadataMap();
        metadataMap.forEach((k, v) -> {
            log.info("k: {}, v: {}", k, v);
        });
        String serverId = clientConnected.serverId();
        String userSessionId = clientConnected.userSessionId();
        int keepAliveTimeSeconds = clientConnected.keepAliveTimeSeconds();
        boolean cleanSession = clientConnected.cleanSession();
        boolean sessionPresent = clientConnected.sessionPresent();
        ClientConnected.WillInfo willInfo = clientConnected.lastWill();
        if (null != willInfo) {
            boolean retain = willInfo.isRetain();
            String topic = willInfo.topic();
            ByteBuffer payload = willInfo.payload();
            int qos = willInfo.qos().getNumber();
        }

    }

    private void handleSubAckedEvent(Event<?> event) {
        SubAcked subAcked = (SubAcked) event.clone();
        executor.execute(() -> sendEventToKafka(TOPIC_MAP.get(event.type()), subAcked.toString()));
    }

    private void handleUnsubAckedEvent(Event<?> event) {
        UnsubAcked unsubAcked = (UnsubAcked) event.clone();
        executor.execute(() -> sendEventToKafka(TOPIC_MAP.get(event.type()), unsubAcked.toString()));
    }


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
                    messageDetails.put("pubQoS", pubQoSValue);
                    messageDetails.put("timestamp", timestamp);
                    messageDetails.put("expireTimestamp", expireTimestamp);
                    messageDetails.put("payload", payloadStr);

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

    private void handleDistErrorEvent(Event<?> event) {
        DistError distError = (DistError) event.clone();
        executor.execute(() -> sendEventToKafka(TOPIC_MAP.get(event.type()), distError.toString()));
    }

    private void handleByClientEvent(Event<?> event) {
        ByClient byClient = (ByClient) event.clone();
        executor.execute(() -> sendEventToKafka(TOPIC_MAP.get(event.type()), byClient.toString()));
    }

    private void handleByServerEvent(Event<?> event) {
        ByServer byServer = (ByServer) event.clone();
        executor.execute(() -> sendEventToKafka(TOPIC_MAP.get(event.type()), byServer.toString()));
    }

    private void handleKickedEvent(Event<?> event) {
        Kicked kicked = (Kicked) event.clone();
        executor.execute(() -> sendEventToKafka(TOPIC_MAP.get(event.type()), kicked.toString()));
    }

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
