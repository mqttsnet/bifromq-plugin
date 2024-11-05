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

import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.mqttsnet.thinglinks.config.PluginConfig;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;

/**
 * -----------------------------------------------------------------------------
 * File Name: BifromqSettingProviderPluginSettingProvider
 * -----------------------------------------------------------------------------
 * Description:
 * <a href="https://bifromq.io/zh-Hans/docs/plugin/setting_provider/">...</a>
 * 自定义运行时变更的设置项
 * <p>
 * 1. 实现ISettingProvider接口
 * 2. 通过@Extension注解标记为插件
 * 3. 实现provide方法，根据setting和tenantId返回对应的设置项
 * 4. 解析处理setting和tenantId，返回对应的设置项
 * <p>
 * -----------------------------------------------------------------------------
 *
 * @author xiaonannet
 * @version 1.0
 * -----------------------------------------------------------------------------
 * Revision History:
 * Date         Author          Version     Description
 * --------      --------     -------   --------------------
 * 2024/2/23       xiaonannet        1.0        Initial creation
 * -----------------------------------------------------------------------------
 * @email
 * @date 2024/2/23 15:36
 */

@Slf4j
@Extension
public final class BifromqSettingProviderPluginSettingProvider implements ISettingProvider {


    private final PluginConfig pluginConfig;

    /**
     * 构造函数，通过 {@link BifromqSettingProviderContext} 初始化配置。
     *
     * @param context {@link BifromqSettingProviderContext} 认证插件的上下文，包含配置信息。
     */
    public BifromqSettingProviderPluginSettingProvider(BifromqSettingProviderContext context) {
        this.pluginConfig = context.getPluginConfig();
    }


    /**
     * 提供指定设置的值。
     *
     * @param setting  设定项
     * @param tenantId 租户 ID
     * @param <R>      返回类型
     * @return 设置的值
     */
    @Override
    public <R> R provide(Setting setting, String tenantId) {
        if (setting == Setting.DebugModeEnabled) {
            Boolean r = checkDebugMode(tenantId);
            log.info("Debug mode for tenant {} is {}", tenantId, r);
            return (R) r;
        }
        return setting.current(tenantId);
    }

    /**
     * 检查指定租户的调试模式。
     *
     * @param tenantId 租户 ID
     * @return 是否启用调试模式
     */
    private boolean checkDebugMode(String tenantId) {
        // 自定义逻辑检查调试模式
        return true; // 示例逻辑，替换为实际实现
    }

    /**
     * 关闭设置提供者，执行必要的清理操作。
     */
    @Override
    public void close() {
        // 自定义关闭逻辑（如果需要）
        ISettingProvider.super.close();
    }
}
