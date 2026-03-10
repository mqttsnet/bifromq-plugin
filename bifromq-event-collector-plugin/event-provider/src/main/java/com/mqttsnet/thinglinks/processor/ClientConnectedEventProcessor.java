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
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientconnected.ClientConnected;
import com.mqttsnet.thinglinks.constant.CommonConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * 客户端连接事件处理器
 * 处理客户端已成功连接到服务器事件并构建相应的数据
 *
 * @author mqttsnet
 * @since 1.0.0
 */
@Slf4j
public class ClientConnectedEventProcessor extends AbstractEventProcessor {

    @Override
    protected Map<String, Object> buildEventData(Event<?> event) {
        Map<String, Object> data = new HashMap<>();

        // 验证事件类型
        if (!(event instanceof ClientConnected)) {
            log.error("Invalid event type. Expected ClientConnected, but got {}", event.getClass().getSimpleName());
            return data;
        }

        Optional.of(event)
                .map(e -> (ClientConnected) e.clone())
                .ifPresent(clientConnected -> {
                    Optional<String> tenantIdOpt = Optional.of(clientConnected.clientInfo().getTenantId());
                    tenantIdOpt.ifPresent(tenantId -> data.put(CommonConstants.TENANT_ID, tenantId));
                    data.put("serverId", getSafeValue(clientConnected.serverId(), ""));
                    data.put("userSessionId", getSafeValue(clientConnected.userSessionId(), ""));
                    data.put("cleanSession", getSafeValue(clientConnected.cleanSession(), false));
                    data.put("sessionPresent", getSafeValue(clientConnected.sessionPresent(), false));
                    data.put("keepAliveTimeSeconds", getSafeValue(clientConnected.keepAliveTimeSeconds(), 0));
                    Optional.ofNullable(clientConnected.lastWill()).ifPresent(lastWill -> {
                        data.put("lastWillTopic", getSafeValue(lastWill.topic(), ""));
                        data.put("lastWillQos", getSafeValue(lastWill.qos().getNumber(), 0));
                        data.put("lastWillRetain", getSafeValue(lastWill.isRetain(), false));
                        data.put("lastWillPayload", getSafeValue(lastWill.payload(), ""));
                    });
                    Optional<Map<String, String>> metadataMapOpt = Optional.of(clientConnected.clientInfo().getMetadataMap());
                    metadataMapOpt.ifPresent(metadataMap -> {
                        data.put("acl", getSafeValue(metadataMap.get("acl"), ""));
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