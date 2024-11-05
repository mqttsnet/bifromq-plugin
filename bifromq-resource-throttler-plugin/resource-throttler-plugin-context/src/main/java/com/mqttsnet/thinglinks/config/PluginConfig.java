package com.mqttsnet.thinglinks.config;

import lombok.Data;

/**
 * PluginConfig 用于封装租户级设置的配置。
 */
@Data
public class PluginConfig {

    /**
     * 是否启用资源限制
     */
    private Boolean isEnableHasResource;
}
