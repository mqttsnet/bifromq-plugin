package bifromq.plugin.config;

import lombok.Data;

@Data
public class EventCollectorConfig {
    private String kafkaBootstrapServer;
}
