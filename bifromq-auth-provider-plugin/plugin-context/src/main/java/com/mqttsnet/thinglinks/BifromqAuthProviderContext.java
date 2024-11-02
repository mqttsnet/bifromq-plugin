package com.mqttsnet.thinglinks;

import cn.hutool.json.JSONUtil;
import com.baidu.bifromq.plugin.BifroMQPluginContext;
import com.baidu.bifromq.plugin.BifroMQPluginDescriptor;
import com.mqttsnet.thinglinks.config.PluginConfig;
import com.mqttsnet.thinglinks.util.ConfigUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class BifromqAuthProviderContext extends BifroMQPluginContext {

    private final BifroMQPluginDescriptor bifroMQPluginDescriptor;
    private final PluginConfig pluginConfig;

    public BifromqAuthProviderContext(BifroMQPluginDescriptor descriptor) {
        super(descriptor);
        this.bifroMQPluginDescriptor = descriptor;
        // 使用ConfigUtil加载配置
        pluginConfig = ConfigUtil.getPluginConfig();
        log.info("Initialized BifromqAuthProviderContext with descriptor: {}", bifroMQPluginDescriptor.getDescriptor().toString());
    }

    @Override
    protected void init() {
        log.info("Starting initialization of BifromqAuthProviderContext for plugin: {}", bifroMQPluginDescriptor.getDescriptor().toString());
        log.debug("Plugin root path: {}", bifroMQPluginDescriptor.getPluginRoot());
        log.debug("Development mode: {}", bifroMQPluginDescriptor.isDevelopment());

        try {
            log.debug("Loading configurations for plugin...");
            // TODO 插入配置加载代码
            log.info("Configurations loaded successfully for {}", bifroMQPluginDescriptor.getDescriptor().toString());
        } catch (Exception e) {
            log.error("Initialization failed for plugin {}: {}", bifroMQPluginDescriptor.getDescriptor().toString(), e.getMessage(), e);
        }
    }

    @Override
    protected void close() {
        log.info("Closing BifromqAuthProviderContext for plugin: {}", bifroMQPluginDescriptor.getDescriptor().toString());

        try {
            log.debug("Releasing resources for plugin...");
            // TODO 插入资源释放代码
            log.info("Resources released successfully for {}", bifroMQPluginDescriptor.getDescriptor().toString());
        } catch (Exception e) {
            log.error("Failed to close BifromqAuthProviderContext for plugin {}: {}", bifroMQPluginDescriptor.getDescriptor().toString(), e.getMessage(), e);
        }
    }

    public PluginConfig getPluginConfig() {
        // 添加调试日志，打印插件描述符信息
        log.info("Attempting to load config file from /conf/config.yaml for plugin: {}", bifroMQPluginDescriptor.getDescriptor().toString());

        if (this.pluginConfig != null) {
            // 打印配置信息
            log.info("Loaded PluginConfig: {}", JSONUtil.toJsonStr(this.pluginConfig));
        } else {
            log.warn("PluginConfig is null. Please check if the configuration file is loaded correctly.");
        }

        return this.pluginConfig;
    }

}