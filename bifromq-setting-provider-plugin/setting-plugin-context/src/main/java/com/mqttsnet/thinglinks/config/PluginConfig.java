package com.mqttsnet.thinglinks.config;

import lombok.Data;

/**
 * PluginConfig 用于封装租户级设置的配置。
 */
@Data
public class PluginConfig {
    /**
     * 启用或禁用调试模式
     */
    private Boolean debugModeEnabled;
}
