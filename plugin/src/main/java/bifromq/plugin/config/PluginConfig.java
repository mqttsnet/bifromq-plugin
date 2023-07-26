package bifromq.plugin.config;

import lombok.Data;

@Data
public class PluginConfig {
    private AuthProviderConfig authProviderConfig;
    private EventCollectorConfig eventCollectorConfig;
}
