package bifromq.plugin;

import bifromq.plugin.config.PluginConfig;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

@Slf4j
public class CompoundPlugin extends Plugin {
    /**
     * Constructor to be used by plugin manager for plugin instantiation. Your plugins have to provide constructor with
     * this exact signature to be successfully loaded by manager.
     *
     * @param wrapper
     */
    private PluginConfig pluginConfig;
    public CompoundPlugin(PluginWrapper wrapper) {
        super(wrapper);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

}
