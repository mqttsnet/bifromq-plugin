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

import java.util.HashMap;
import java.util.Map;

import com.baidu.bifromq.plugin.eventcollector.EventType;
import lombok.extern.slf4j.Slf4j;

/**
 * 事件处理器工厂
 * 根据事件类型创建并返回对应的事件处理器实例
 */
@Slf4j
public class EventProcessorFactory {

    private final Map<EventType, EventProcessor> processorMap = new HashMap<>();

    /**
     * 构造函数
     * 初始化所有事件处理器
     */
    public EventProcessorFactory() {
        // 注册各种事件处理器
        processorMap.put(EventType.CLIENT_CONNECTED, new ClientConnectedEventProcessor());
        processorMap.put(EventType.SUB_ACKED, new SubAckedEventProcessor());
        processorMap.put(EventType.UNSUB_ACKED, new UnsubAckedEventProcessor());
        processorMap.put(EventType.DISTED, new DistedEventProcessor());
        processorMap.put(EventType.DIST_ERROR, new DistErrorEventProcessor());
        processorMap.put(EventType.BY_CLIENT, new ByClientEventProcessor());
        processorMap.put(EventType.BY_SERVER, new ByServerEventProcessor());
        processorMap.put(EventType.KICKED, new KickedEventProcessor());
        processorMap.put(EventType.PING_REQ, new PingReqEventProcessor());
    }

    /**
     * 根据事件类型获取对应的处理器
     * @param eventType 事件类型
     * @return 事件处理器实例，如果没有找到对应处理器则返回null
     */
    public EventProcessor getProcessor(EventType eventType) {
        EventProcessor processor = processorMap.get(eventType);
        if (processor == null) {
            log.warn("No processor found for event type: {}", eventType);
        }
        return processor;
    }

}