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
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByServer;
import com.mqttsnet.thinglinks.constant.CommonConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * 服务器断开连接事件处理器
 * 处理服务器主动断开客户端连接事件并构建相应的数据
 * 服务器由于某些原因（如客户端违反协议规定）主动断开了与客户端的连接。
 *
 * @author mqttsnet
 * @since 1.0.0
 */
@Slf4j
public class ByServerEventProcessor extends AbstractEventProcessor {

    @Override
    protected Map<String, Object> buildEventData(Event<?> event) {
        Map<String, Object> data = new HashMap<>();

        // 验证事件类型
        if (!(event instanceof ByServer)) {
            log.error("Invalid event type. Expected ByServer, but got {}", event.getClass().getSimpleName());
            return data;
        }
        Optional.of(event)
                .map(e -> (ByServer) e.clone())
                .ifPresent(byServer -> {
                    data.put(CommonConstants.TENANT_ID, getSafeValue(byServer.clientInfo().getTenantId(), ""));

                    Optional<Map<String, String>> metadataMapOpt = Optional.of(byServer.clientInfo().getMetadataMap());
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