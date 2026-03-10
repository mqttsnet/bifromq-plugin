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

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.mqttsnet.thinglinks.config.EventCollectorConfig;
import com.mqttsnet.thinglinks.config.PluginConfig;
import com.mqttsnet.thinglinks.processor.EventProcessor;
import com.mqttsnet.thinglinks.processor.EventProcessorFactory;
import com.mqttsnet.thinglinks.sender.KafkaMessageSender;
import com.mqttsnet.thinglinks.util.TaskQueue;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;


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
 * @version 1.1
 * -----------------------------------------------------------------------------
 * Revision History:
 * Date         Author          Version     Description
 * --------      --------     -------   --------------------
 * 2024/2/23       xiaonannet        1.0        Initial creation
 * 2025/3/6        xiaonannet        1.1        Add event enumeration
 * -----------------------------------------------------------------------------
 * @email
 * @date 2024/2/23 15:36
 */
@Slf4j
@Extension
public final class BifromqEventCollectorPluginEventProvider implements IEventCollector {

    private final KafkaMessageSender sender;
    private final EventProcessorFactory processorFactory;
    private final TaskQueue taskQueue = new TaskQueue(8, 100, 60L, TimeUnit.SECONDS);

    private static final Map<EventType, String> TOPIC_MAP = new EnumMap<>(EventType.class);

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
        TOPIC_MAP.put(EventType.NOT_AUTHORIZED_CLIENT, "mqtt.client.unauthorized");
        TOPIC_MAP.put(EventType.MQTT_SESSION_START, "mqtt.session.start");
        TOPIC_MAP.put(EventType.MQTT_SESSION_STOP, "mqtt.session.stop");
    }

    public BifromqEventCollectorPluginEventProvider(BifromqEventCollectorContext context) {
        // 通过 context 获取配置
        PluginConfig pluginConfig = context.getPluginConfig();
        EventCollectorConfig eventCollectorConfig = pluginConfig.getEventCollectorConfig();
        log.info("EventProvider initialized with Kafka server: {}", eventCollectorConfig.getKafkaBootstrapServer());

        // 初始化Kafka消息发送器
        this.sender = new KafkaMessageSender(eventCollectorConfig);
        // 初始化事件处理器工厂
        this.processorFactory = new EventProcessorFactory();
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

            String topic = TOPIC_MAP.get(event.type());
            EventProcessor processor = processorFactory.getProcessor(event.type());

            if (processor != null) {
                long startTime = System.currentTimeMillis();
                log.info("Starting execution of event: '{}'. Time: {}", event.type(), startTime);

                processor.process(event, topic, sender);

                long endTime = System.currentTimeMillis();
                log.info("Completed execution of event: '{}'. Duration: {} ms", event.type(), endTime - startTime);
            } else {
                log.warn("No processor found for event type: {}", event.type());
            }
        });
    }


    @Override
    public void close() {
        taskQueue.shutdown();
        sender.close();
    }


}
