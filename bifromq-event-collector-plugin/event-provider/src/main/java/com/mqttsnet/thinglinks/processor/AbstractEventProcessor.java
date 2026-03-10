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

package com.mqttsnet.thinglinks.processor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

import com.baidu.bifromq.plugin.eventcollector.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mqttsnet.thinglinks.constant.CommonConstants;
import com.mqttsnet.thinglinks.enumeration.EventTypeEnum;
import com.mqttsnet.thinglinks.sender.KafkaMessageSender;
import lombok.extern.slf4j.Slf4j;

/**
 * 抽象事件处理器
 * 提供通用的事件处理逻辑和辅助方法
 *
 * @author mqttsnet
 * @since 1.0.0
 */
@Slf4j
public abstract class AbstractEventProcessor implements EventProcessor {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void process(Event<?> event, String topic, KafkaMessageSender sender) {
        log.info("Processing event type: {}", event.type());
        try {
            Map<String, Object> eventData = buildEventData(event);
            enrichEventData(eventData, event);
            String message = OBJECT_MAPPER.writeValueAsString(eventData);
            sender.send(topic, message);
            log.info("Sent event to Kafka topic: {}, data: {}", topic, message);
        } catch (Exception e) {
            log.error("Error processing event: {}", event.type(), e);
        }
    }

    /**
     * 构建事件数据
     *
     * @param event 事件对象
     * @return 事件数据映射
     */
    protected abstract Map<String, Object> buildEventData(Event<?> event);

    /**
     * 丰富事件数据
     *
     * @param eventData 事件数据
     * @param event     事件对象
     */
    private void enrichEventData(Map<String, Object> eventData, Event<?> event) {
        // 添加事件类型
        eventData.put(CommonConstants.EVENT_TYPE, EventTypeEnum.byValue(event.type().name()).get().getBusinessSystemEventType());

        // 添加时间戳（毫秒）
        long timestamp = System.currentTimeMillis();
        eventData.put(CommonConstants.EVENT_TIME, timestamp);

        // 添加格式化的时间
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        eventData.put(CommonConstants.EVENT_TIME_STR, dateTime.format(DATE_TIME_FORMATTER));

        // 添加成功标志
        eventData.put(CommonConstants.SUCCESS, CommonConstants.SUCCESS);
    }

    /**
     * 安全地获取值，避免空指针
     *
     * @param value        原始值
     * @param defaultValue 默认值
     * @return 值或默认值
     */
    protected <T> T getSafeValue(T value, T defaultValue) {
        return Optional.ofNullable(value).orElse(defaultValue);
    }

}