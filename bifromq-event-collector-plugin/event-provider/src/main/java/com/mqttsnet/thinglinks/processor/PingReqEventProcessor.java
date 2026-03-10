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
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.PingReq;
import com.mqttsnet.thinglinks.constant.CommonConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * PING请求事件处理器
 * 处理PING请求事件并构建相应的数据
 *
 * @author mqttsnet
 * @since 1.0.0
 */
@Slf4j
public class PingReqEventProcessor extends AbstractEventProcessor {

    @Override
    protected Map<String, Object> buildEventData(Event<?> event) {
        Map<String, Object> data = new HashMap<>();

        // 确保事件是 PingReq 类型
        if (!(event instanceof PingReq)) {
            log.error("Invalid event type. Expected PingReq, but got {}", event.getClass().getSimpleName());
            return data;
        }

        Optional.of(event)
                .map(e -> (PingReq) e.clone())
                .ifPresent(pingReq -> {
                    data.put(CommonConstants.TENANT_ID, getSafeValue(pingReq.clientInfo().getTenantId(), ""));
                    data.put("pong", getSafeValue(pingReq.pong(), true));
                    Optional<Map<String, String>> metadataMapOpt = Optional.of(pingReq.clientInfo().getMetadataMap());
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
                        data.put("heartbeatTime", System.currentTimeMillis());
                    });
                });

        return data;
    }

}