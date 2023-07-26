package bifromq.plugin.provider;

import bifromq.plugin.config.ConfigUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.StrUtil;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.distservice.Delivered;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ConnectTimeout;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Kicked;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.subhandling.SubAcked;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pf4j.Extension;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

    // This is the mapping process for pushing event-type messages to kafka Topic
    static {
        TOPIC_MAP.put(EventType.CLIENT_CONNECTED, "ClientConnectedTopic");
        TOPIC_MAP.put(EventType.SUB_ACKED, "SubAckedTopic");
        TOPIC_MAP.put(EventType.UNSUB_ACKED, "UnsubAckedTopic");
        TOPIC_MAP.put(EventType.DISTED, "DistedTopic");
        TOPIC_MAP.put(EventType.DELIVERED, "DeliveredTopic");
        TOPIC_MAP.put(EventType.BY_CLIENT, "ByClientTopic");
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
            log.warn("Events of type {} are discarded.", event.type());
            return;
        }

        switch (event.type()) {
            case CONNECT_TIMEOUT:
                handleConnectTimeoutEvent(event);
                break;
            case KICKED:
                handleKickedEvent(event);
                break;
            case SUB_ACKED:
                handleSubAckedEvent(event);
                break;
            case DELIVERED:
                handleDeliveredEvent(event);
                break;
        }
    }

    private void handleConnectTimeoutEvent(Event<?> event) {
        ConnectTimeout connectTimeout = (ConnectTimeout) event.clone();
        executor.execute(() -> sendEventToKafka(TOPIC_MAP.get(event.type()), connectTimeout.toString()));
    }

    private void handleKickedEvent(Event<?> event) {
        Kicked kicked = (Kicked) event.clone();
        executor.execute(() -> sendEventToKafka(TOPIC_MAP.get(event.type()), kicked.toString()));
    }

    private void handleSubAckedEvent(Event<?> event) {
        SubAcked subAcked = (SubAcked) event.clone();
        executor.execute(() -> sendEventToKafka(TOPIC_MAP.get(event.type()), subAcked.toString()));
    }

    private void handleDeliveredEvent(Event<?> event) {
        Delivered delivered = (Delivered) event.clone();
        TopicMessagePack messagePack = delivered.messages();
        String topic = messagePack.getTopic();
        List<TopicMessagePack.PublisherPack> publisherPacks = messagePack.getMessageList();
        publisherPacks.forEach(pack -> {
            List<Message> messageList = pack.getMessageList();
            messageList.forEach(msg -> {
                long messageId = msg.getMessageId();
                ByteString payload = msg.getPayload();
                log.info("messageId---------------:{}", messageId);
                // You may process messageId and payload here if needed
                // Then, convert the ByteString payload to String
                String payloadStr = payload.toStringUtf8();
                // Send the payloadStr to Kafka
                sendEventToKafka(topic, payloadStr);
            });
        });
    }

    private void sendEventToKafka(String topic, String message) {
        if (CharSequenceUtil.isBlank(topic) || CharSequenceUtil.isBlank(message)) {
            log.warn("Cannot send null event to Kafka.");
            return;
        }
        producer.send(new ProducerRecord<>(topic, message), (recordMetadata, e) -> {
            if (e != null) {
                log.error("Error while producing message to topic {}. Exception: ", recordMetadata.topic(), e);
            } else {
                log.info("Successfully sent message to topic {}.", recordMetadata.topic());
            }
        });

    }
}