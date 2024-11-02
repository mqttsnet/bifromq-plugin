# BifroMQ Event Provider Plugin

这是一个基于 BifroMQ 框架的事件提供器插件，用于收集 BifroMQ 执行过程中发生的各种事件。该插件允许开发者灵活处理事件并实现特定的事件过滤逻辑。

## 目录结构

```plaintext
bifromq-event-provider-plugin/
├── event-provider/ <-- event provider module as a reference for other bifromq plugin, you can remove it if not needed
│   └── src/
│       └── main/
│           └── java/
│               └── com.yourcompany.newproject/
│                   └── BifromqEventCollectorPluginEventProvider.java
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

- **事件收集**：收集 BifroMQ 执行过程中发生的各种事件。
- **事件过滤**：支持通过自定义过滤逻辑对事件进行筛选。
- **事件处理**：提供灵活的事件处理机制，允许开发者自定义事件处理逻辑。
- **配置驱动**：支持通过 `config.yaml` 进行动态配置。
- **日志管理**：集成 Logback 日志框架，通过 `logback.xml` 进行日志级别和格式的配置。

## 快速开始

### 1. 配置文件设置

在 plugin-context 模块`conf/config.yaml` 中定义事件提供器配置，具体配置项如下：

```yaml
eventProviderConfig:
  # The kafka topic to which the events are published
  kafkaBootstrapServer: "127.0.0.1:9200"
```

### 2. 构建和打包

通过 Maven 构建并打包插件：

```bash
mvn clean package
```

构建成功后，插件包将生成在 `target/` 目录中，例如 `target/bifromq-event-provider-plugin-1.0.0.zip`。

### 3. 部署和加载插件

将插件包解压到 BifroMQ 插件目录（假设目录为 `/path/to/bifromq/plugins`）：

```bash
unzip target/bifromq-event-provider-plugin-1.0.0.zip -d /path/to/bifromq/plugins/
```

然后启动 BifroMQ 服务，插件将自动加载。

在 BifroMQ 中运行此插件时，并不需要通过配置文件指定 Event Provider 的完全限定类名（FQN）。请注意，BifroMQ 允许运行多个 Event
Provider 实例。

## 使用说明

### 代码示例

#### 初始化事件上下文

在插件初始化时，通过 `BifromqEventProviderContext` 加载配置：

```java
public class YourPluginClassNameEventProvider {
    private final int collectionInterval;

    public YourPluginClassNameEventProvider(BifromqEventProviderContext context) {
        this.collectionInterval = context.getPluginConfig().getEventProviderConfig().getCollectionInterval();
        // 初始化其他配置项
    }
```

#### 收集事件

在插件中实现事件收集逻辑：

```java
public void collectEvents(){
        // 实现事件收集逻辑
        }
```

#### 处理事件

在插件中实现事件处理逻辑：

```java
public void processEvents(){
        // 实现事件处理逻辑
        }
```

## 常见问题

### 如何在不重启 BifroMQ 的情况下更新配置？

可以直接修改 `config.yaml` 中的配置项，并重启插件实现动态更新。

### 如何启用详细日志？

修改 `logback.xml` 中的 `<level>` 标签，将日志级别设置为 `DEBUG`，然后重启插件。

## 贡献

欢迎提出 issue 和 pull request 以改进插件功能。如有疑问，请联系项目维护社区 MQTTSNET。

--- 

