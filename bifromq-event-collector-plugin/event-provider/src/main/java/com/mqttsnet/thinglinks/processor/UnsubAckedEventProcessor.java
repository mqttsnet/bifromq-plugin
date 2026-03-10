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
import java.util.Optional;

import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.subhandling.UnsubAcked;
import com.mqttsnet.thinglinks.constant.CommonConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * 取消订阅确认事件处理器
 * 处理取消订阅确认事件并构建相应的数据
 *
 * @author mqttsnet
 * @since 1.0.0
 */
@Slf4j
public class UnsubAckedEventProcessor extends AbstractEventProcessor {

    @Override
    protected Map<String, Object> buildEventData(Event<?> event) {
        Map<String, Object> data = new HashMap<>();

        // 验证事件类型
        if (!(event instanceof UnsubAcked)) {
            log.error("Invalid event type. Expected UnsubAcked, but got {}", event.getClass().getSimpleName());
            return new HashMap<>();
        }

        Optional.of(event)
                .map(e -> (UnsubAcked) e.clone())
                .ifPresent(unsubAcked -> {
                    Optional<String> tenantIdOpt = Optional.of(unsubAcked.clientInfo().getTenantId());
                    tenantIdOpt.ifPresent(tenantId -> data.put(CommonConstants.TENANT_ID, getSafeValue(tenantId, "")));

                    data.put("messageId", getSafeValue(unsubAcked.messageId(), ""));

                    Optional.ofNullable(unsubAcked.topicFilter())
                            .filter(topics -> !topics.isEmpty())
                            .map(topics -> topics.get(0))
                            .ifPresent(topic -> data.put(CommonConstants.TOPIC, getSafeValue(topic, "")));

                    Optional<Map<String, String>> metadataMapOpt = Optional.of(unsubAcked.clientInfo().getMetadataMap());
                    metadataMapOpt.ifPresent(metadataMap -> {
                        data.put(CommonConstants.ACL_RULE, getSafeValue(metadataMap.get(CommonConstants.ACL_RULE), ""));
                        data.put("ver", getSafeValue(metadataMap.get("ver"), ""));
                        data.put(CommonConstants.CLIENT_ID, getSafeValue(metadataMap.get(CommonConstants.CLIENT_ID), ""));
                        data.put(CommonConstants.USER_ID, getSafeValue(metadataMap.get(CommonConstants.USER_ID), ""));
                        data.put("channelId", getSafeValue(metadataMap.get("channelId"), ""));
                        data.put("address", getSafeValue(metadataMap.get("address"), ""));
                        data.put("broker", getSafeValue(metadataMap.get("broker"), ""));
                        data.put(CommonConstants.VERSION, getSafeValue(metadataMap.get("ver"), ""));
                        data.put("sessionType", getSafeValue(metadataMap.get("sessionType"), ""));
                    });
                });
        return data;
    }

}