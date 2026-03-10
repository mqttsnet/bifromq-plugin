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

import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.bifromq.plugin.resourcethrottler.TenantResourceType;
import com.mqttsnet.thinglinks.config.PluginConfig;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;

/**
 * 提供租户级别的指标来衡量资源的数量和速率，包括计数器、仪表和摘要指标。
 *
 * @author mqttsnet
 */
@Slf4j
@Extension
public class BifromqResourceThrottlerPluginResourceThrottlerProvider implements IResourceThrottler {


    private final PluginConfig pluginConfig;

    /**
     * 构造函数，通过 {@link BifromqResourceThrottlerContext} 初始化配置。
     *
     * @param context {@link BifromqResourceThrottlerContext} 插件的上下文，包含配置信息。
     */
    public BifromqResourceThrottlerPluginResourceThrottlerProvider(BifromqResourceThrottlerContext context) {
        this.pluginConfig = context.getPluginConfig();
    }

    /**
     * Determine if the tenant has enough resource of given type
     *
     * @param tenantId the id of the tenant
     * @param type     the resource type
     * @return true for there is enough resource of given type for the tenant
     */
    @Override
    public boolean hasResource(String tenantId, TenantResourceType type) {
        //此方法在BifroMQ的工作线程被上同步调用，需要确保其实现的高效，以避免影响BifroMQ性能。
        // 当方法返回false时将产生限制行为，同时生成一个ResourceThrottling事件，并报告给Event Collector
        log.info("hasResource tenantId={}, type={}", tenantId, type);
        // TODO 请求 资源限制服务，判断租户是否有足够的资源

        return true;
    }

    @Override
    public void close() {
        IResourceThrottler.super.close();
    }
}