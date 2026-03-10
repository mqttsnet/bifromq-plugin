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

package com.mqttsnet.thinglinks.sender;

import java.util.Optional;
import java.util.Properties;

import cn.hutool.core.text.CharSequenceUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Kafka消息发送器
 * 封装Kafka生产者的创建和消息发送逻辑
 *
 * @author mqttsnet
 * @since 1.0.0
 */
@Slf4j
public class KafkaMessageSender {

    private final KafkaProducer<String, String> producer;


    /**
     * 构造函数 - 使用配置构建器
     *
     * @param config 事件采集器配置
     */
    public KafkaMessageSender(com.mqttsnet.thinglinks.config.EventCollectorConfig config) {
        if (config == null || config.getKafka() == null) {
            throw new IllegalArgumentException("Kafka配置不能为空");
        }

        // 验证配置有效性
        if (!config.isValid()) {
            throw new IllegalArgumentException("Kafka配置不完整或无效");
        }

        Properties kafkaProperties = buildPropertiesFromConfig(config);
        this.producer = new KafkaProducer<>(kafkaProperties);
        log.info("✅ Kafka生产者初始化完成 - 服务器: {}", config.getKafkaBootstrapServer());
    }

    /**
     * 安全构建Kafka配置属性
     *
     * @param config 事件采集器配置
     * @return Kafka配置属性
     */
    private Properties buildPropertiesFromConfig(com.mqttsnet.thinglinks.config.EventCollectorConfig config) {
        Properties props = new Properties();

        // 基础配置 - 必须项
        String bootstrapServers = Optional.ofNullable(config.getKafkaBootstrapServer())
                .orElseThrow(() -> new IllegalArgumentException("Kafka服务器地址不能为空"));
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // 序列化配置 - 使用安全获取
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 安全处理嵌套配置
        Optional.ofNullable(config.getKafka())
                .map(com.mqttsnet.thinglinks.config.EventCollectorConfig.KafkaConfig::getProducer)
                .ifPresent(producerConfig -> {
                    // SASL认证配置
                    applySASLConfig(props, producerConfig);

                    // 批量配置
                    applyBatchConfig(props, producerConfig);

                    // 可靠性配置
                    applyReliabilityConfig(props, producerConfig);

                    // 超时配置
                    applyTimeoutConfig(props, producerConfig);

                    // 重连配置
                    applyReconnectConfig(props, producerConfig);

                    // 其他配置
                    applyOtherConfig(props, producerConfig);
                });

        logKafkaConfig(props);
        return props;
    }

    /**
     * 应用SASL配置
     *
     * @param props          配置属性
     * @param producerConfig 生产者配置
     */
    private void applySASLConfig(Properties props,
                                 com.mqttsnet.thinglinks.config.EventCollectorConfig.ProducerConfig producerConfig) {
        Optional.ofNullable(producerConfig.getSecurityProtocol())
                .filter(CharSequenceUtil::isNotBlank)
                .ifPresent(protocol -> {
                    props.put("security.protocol", protocol);

                    Optional.ofNullable(producerConfig.getSasl())
                            .ifPresent(sasl -> {
                                Optional.ofNullable(sasl.getMechanism())
                                        .filter(CharSequenceUtil::isNotBlank)
                                        .ifPresent(mechanism -> props.put("sasl.mechanism", mechanism));

                                Optional.ofNullable(sasl.getJaas())
                                        .map(com.mqttsnet.thinglinks.config.EventCollectorConfig.JaasConfig::getConfig)
                                        .filter(CharSequenceUtil::isNotBlank)
                                        .ifPresent(jaas -> props.put("sasl.jaas.config", jaas));
                            });
                });
    }

    /**
     * 应用批量配置
     *
     * @param props          配置属性
     * @param producerConfig 生产者配置
     */
    private void applyBatchConfig(Properties props,
                                  com.mqttsnet.thinglinks.config.EventCollectorConfig.ProducerConfig producerConfig) {
        Optional.ofNullable(producerConfig.getBatchSize())
                .ifPresent(size -> props.put(ProducerConfig.BATCH_SIZE_CONFIG, size));

        Optional.ofNullable(producerConfig.getLingerMs())
                .ifPresent(linger -> props.put(ProducerConfig.LINGER_MS_CONFIG, linger));

        Optional.ofNullable(producerConfig.getBufferMemory())
                .ifPresent(memory -> props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, memory));
    }

    /**
     * 应用可靠性配置
     *
     * @param props          配置属性
     * @param producerConfig 生产者配置
     */
    private void applyReliabilityConfig(Properties props,
                                        com.mqttsnet.thinglinks.config.EventCollectorConfig.ProducerConfig producerConfig) {
        Optional.ofNullable(producerConfig.getAcks())
                .filter(CharSequenceUtil::isNotBlank)
                .ifPresent(acks -> props.put(ProducerConfig.ACKS_CONFIG, acks));

        Optional.ofNullable(producerConfig.getRetries())
                .ifPresent(retries -> props.put(ProducerConfig.RETRIES_CONFIG, retries));

        Optional.ofNullable(producerConfig.getEnableIdempotence())
                .ifPresent(idempotence -> props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence));
    }

    /**
     * 应用超时配置
     *
     * @param props          配置属性
     * @param producerConfig 生产者配置
     */
    private void applyTimeoutConfig(Properties props,
                                    com.mqttsnet.thinglinks.config.EventCollectorConfig.ProducerConfig producerConfig) {
        Optional.ofNullable(producerConfig.getRequestTimeoutMs())
                .ifPresent(timeout -> props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout));

        Optional.ofNullable(producerConfig.getDeliveryTimeoutMs())
                .ifPresent(timeout -> props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, timeout));

        Optional.ofNullable(producerConfig.getMaxBlockMs())
                .ifPresent(block -> props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, block));
    }

    /**
     * 应用重连配置
     *
     * @param props          配置属性
     * @param producerConfig 生产者配置
     */
    private void applyReconnectConfig(Properties props,
                                      com.mqttsnet.thinglinks.config.EventCollectorConfig.ProducerConfig producerConfig) {
        Optional.ofNullable(producerConfig.getRetryBackoffMs())
                .ifPresent(backoff -> props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, backoff));

        Optional.ofNullable(producerConfig.getReconnectBackoffMs())
                .ifPresent(backoff -> props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, backoff));

        Optional.ofNullable(producerConfig.getReconnectBackoffMaxMs())
                .ifPresent(maxBackoff -> props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, maxBackoff));
    }

    /**
     * 应用其他配置
     *
     * @param props          配置属性
     * @param producerConfig 生产者配置
     */
    private void applyOtherConfig(Properties props,
                                  com.mqttsnet.thinglinks.config.EventCollectorConfig.ProducerConfig producerConfig) {
        Optional.ofNullable(producerConfig.getCompressionType())
                .filter(CharSequenceUtil::isNotBlank)
                .ifPresent(compression -> props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression));

        Optional.ofNullable(producerConfig.getMetadataMaxAgeMs())
                .ifPresent(age -> props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, age));
    }

    /**
     * 发送消息到Kafka
     *
     * @param topic   主题名称
     * @param message 消息内容
     */
    public void send(String topic, String message) {
        if (CharSequenceUtil.isBlank(topic) || CharSequenceUtil.isBlank(message)) {
            log.warn("无法发送空消息到Kafka - Topic: {}, Message: {}", topic, message);
            return;
        }

        try {
            producer.send(new ProducerRecord<>(topic, message), (recordMetadata, e) -> {
                if (e != null) {
                    log.error("发送消息到主题 {} 失败 - 异常: {}", topic, e.getMessage());
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("消息成功发送到主题 {} - 分区: {}, 偏移量: {}",
                                recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.offset());
                    }
                }
            });
        } catch (Exception e) {
            log.error("发送Kafka消息时发生异常 - Topic: {}, Error: {}", topic, e.getMessage());
        }
    }

    /**
     * 同步发送消息到Kafka
     *
     * @param topic   主题名称
     * @param message 消息内容
     * @return 发送是否成功
     */
    public boolean sendSync(String topic, String message) {
        if (CharSequenceUtil.isBlank(topic) || CharSequenceUtil.isBlank(message)) {
            log.warn("无法发送空消息到Kafka - Topic: {}, Message: {}", topic, message);
            return false;
        }

        try {
            producer.send(new ProducerRecord<>(topic, message)).get();
            log.debug("同步消息成功发送到主题: {}", topic);
            return true;
        } catch (Exception e) {
            log.error("同步发送消息到主题 {} 失败 - 异常: {}", topic, e.getMessage());
            return false;
        }
    }

    /**
     * 关闭Kafka生产者
     */
    public void close() {
        try {
            producer.close();
            log.info("Kafka生产者已安全关闭");
        } catch (Exception e) {
            log.error("关闭Kafka生产者时发生异常: {}", e.getMessage());
        }
    }

    /**
     * 打印Kafka配置信息
     */
    private void logKafkaConfig(Properties properties) {
        if (log.isInfoEnabled()) {
            log.info("Kafka生产者配置:");
            log.info("  - Bootstrap Servers: {}", properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            log.info("  - Batch Size: {}", properties.get(ProducerConfig.BATCH_SIZE_CONFIG));
            log.info("  - Linger Ms: {}", properties.get(ProducerConfig.LINGER_MS_CONFIG));
            log.info("  - Buffer Memory: {}", properties.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
            log.info("  - Acks: {}", properties.get(ProducerConfig.ACKS_CONFIG));
            log.info("  - Retries: {}", properties.get(ProducerConfig.RETRIES_CONFIG));
            if (properties.containsKey("security.protocol")) {
                log.info("  - Security Protocol: {}", properties.get("security.protocol"));
            }
        }
    }
}