# BifroMQ Setting Provider Plugin

BifroMQ 定义了一类租户级设置 (Settings)，允许在运行时修改，以实现针对单个租户动态调整 BifroMQ 的服务行为。Setting Provider
Plugin 的目的是在运行时为这些设置提供自定义值。

## 目录结构

```plaintext
bifromq-setting-provider-plugin/
├── setting-provider/ <-- setting provider module as a reference for other bifromq plugin, you can remove it if not needed
│   └── src/
│       └── main/
│           └── java/
│               └── com.yourcompany.newproject/
│                   └── YourPluginClassNameSettingProvider.java
├── plugin-build/  <-- plugin-build module to build the plugin zip file
│   ├── assembly/
│   │   └── assembly-zip.xml
│   ├── conf/      <-- folder to contain plugin configuration files
│   │   ├── config.yaml <-- plugin configuration file
│   │   └── logback.xml <-- logback configuration file for the plugin
│   ├── src/
│   │   └── main/
│   │       └── java/
│   │           └── com.yourcompany.newproject/
│   │               └── YourPluginClassName.java <-- Your plugin main class
│   └── target/
│       └── pom.xml
├── plugin-context/  <-- plugin-context module to define the plugin context
│   └── src/
│       └── main/
│           └── java/
│               └── com.yourcompany.newproject/
│                   └── YourPluginContextClassName.java
└── pom.xml
```

## 功能特性

- **动态设置**：支持在运行时动态提供和修改租户级设置。
- **配置驱动**：支持通过 `config.yaml` 进行动态配置。
- **日志管理**：集成 Logback 日志框架，通过 `logback.xml` 进行日志级别和格式的配置。

## 快速开始

### 1. 配置文件设置

在 plugin-context 模块`conf/config.yaml` 中定义认证和授权配置，具体配置项如下：

```yaml
settingProviderConfig:
  # 自定义设置键值对
  customSettingKey: "customValue"
```

### 2. 构建和打包

通过 Maven 构建并打包插件：

```bash
mvn clean package
```

构建成功后，插件包将生成在 `target/` 目录中，例如 `target/bifromq-setting-provider-plugin-1.0.0.zip`。

### 3. 部署和加载插件

将插件包解压到 BifroMQ 插件目录（假设目录为 `/path/to/bifromq/plugins`）：

```bash
unzip target/bifromq-setting-provider-plugin-1.0.0.zip -d /path/to/bifromq/plugins/
```

然后启动 BifroMQ 服务，插件将自动加载。

在 BifroMQ 中运行此插件时，需要通过配置文件指定 Setting Provider 的完全限定类名（FQN）。请注意，BifroMQ 一次只允许运行一个
Setting Provider 实例。

在 BifroMQ 配置文件 `standalone.yml` 中添加以下内容：

```yaml
settingProviderFQN: com.mqttsnet.thinglinks.BifromqSettingProviderPluginSettingProvider
```

## 使用说明

### 代码示例

#### 初始化认证上下文

在插件初始化时，通过 `BifromqSettingProviderContext` 加载配置：

```java
public class BifromqSettingProviderContext {
    private final String customSetting;

    public BifromqSettingProviderContext(BifromqSettingProviderContext context) {
        this.customSetting = context.getPluginConfig().getSettingProviderConfig().getCustomSettingKey();
        // 初始化其他配置项
    }
}

```

## 常见问题

### 如何在不重启 BifroMQ 的情况下更新配置？

可以直接修改 BifroMQ 配置文件 `standalone.yml` 中的配置项，并重启插件实现动态更新。
程序会自动覆盖模块 `conf/config.yaml` 中定义的配置（standalone.yml 优先级高于 config.yaml）

### 如何启用详细日志？

修改 `logback.xml` 中的 `<level>` 标签，将日志级别设置为 `DEBUG`，然后重启插件。

## 贡献

欢迎提出 issue 和 pull request 以改进插件功能。如有疑问，请联系项目维护社区 MQTTSNET。

--- 
