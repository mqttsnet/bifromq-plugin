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

import com.baidu.bifromq.plugin.eventcollector.Event;
import com.mqttsnet.thinglinks.sender.KafkaMessageSender;

/**
 * 事件处理器接口
 * 定义处理不同类型事件的标准方法
 *
 * @author mqttsnet
 * @since 1.0.0
 */
public interface EventProcessor {

    /**
     * 处理事件并发送到Kafka
     *
     * @param event  事件对象
     * @param topic  Kafka主题
     * @param sender Kafka消息发送器
     */
    void process(Event<?> event, String topic, KafkaMessageSender sender);

}