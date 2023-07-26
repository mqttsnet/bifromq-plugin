package bifromq.plugin.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.beanutils.PropertyUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

public class ConfigUtil {
    public static PluginConfig getPluginConfig() {
        File configFile;
        String resource = "/config.yaml";
        URL res = ConfigUtil.class.getResource(resource);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            if (res.getProtocol().equals("jar")) {
                InputStream input = ConfigUtil.class.getResourceAsStream(resource);
                configFile = File.createTempFile("tempfile", ".tmp");
                OutputStream out = new FileOutputStream(configFile);
                int read;
                byte[] bytes = new byte[1024];

                while ((read = input.read(bytes)) != -1) {
                    out.write(bytes, 0, read);
                }
                out.close();
                configFile.deleteOnExit();
            } else {
                configFile = new File(res.getFile());
            }

            if (configFile != null && !configFile.exists()) {
                throw new RuntimeException("Error: File " + configFile + " not found");
            }
            PluginConfig source = getOverwriteConfig();
            PluginConfig dest = mapper.readValue(configFile, PluginConfig.class);
            if (source != null) {
                PropertyUtils.describe(source).entrySet().stream()
                        .filter(entry -> entry.getValue() != null)
                        .forEach(each -> {
                            try {
                                PropertyUtils.setProperty(dest, each.getKey(), each.getValue());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            return dest;
        } catch (Exception e) {
            throw new RuntimeException("Unable to read starter config file: ", e);
        }
    }

    private static PluginConfig getOverwriteConfig() {
        try {
            File file = new File("./conf/standalone.yml");
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            PluginConfig pluginConfig = mapper.readValue(file, PluginConfig.class);
            return pluginConfig;
        }catch (Exception exception) {
            return null;
        }
    }
}
