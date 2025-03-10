# BifroMQ Auth Provider Plugin

这是一个基于 BifroMQ 框架的认证提供器插件，用于处理 MQTT 客户端的认证和授权。该插件支持多种认证方式，并通过配置文件支持自定义属性和
ACL 控制。

## 目录结构

```plaintext
bifromq-auth-provider-plugin/
├── auth-provider/ <-- auth provider module as a reference for other bifromq plugin, you can remove it if not needed
│   └── src/
│       └── main/
│           └── java/
│               └── com.yourcompany.newproject/
│                   └── YourPluginClassNameAuthProvider.java
├── plugin-build/  <-- plugin-build module to build the plugin zip file
│   ├── assembly/
│   │   └── assembly-zip.xml
│   ├── conf/      <-- folder to contain plugin configuration files
│   │   ├── config.yaml <-- plugin configuration file
│   │   └── logback.xml <-- logback configuration file for the plugin
│   ├── src/
│   │   └── main/
│   │       └── java/
│   │           └──com.yourcompany.newproject/
│   │               └── YourPluginClassName.java <-- Your plugin main class
│   └── target/
│       └── pom.xml
├── plugin-context/  <-- plugin-context module to define the plugin context
│   └── src/
│       └── main/
│           └── java/
│               └── ─com.yourcompany.newproject/
│                   └──YourPluginContextClassName.java
└── pom.xml
```

## 功能特性

- **认证支持**：支持基于用户名/密码和证书的多种认证方式。
- **ACL 控制**：支持通过自定义 ACL (Access Control List) 进行精细的访问权限控制。
- **配置驱动**：支持通过 `config.yaml` 进行动态配置。
- **日志管理**：集成 Logback 日志框架，通过 `logback.xml` 进行日志级别和格式的配置。

## 快速开始

### 1. 配置文件设置

在 plugin-context 模块`conf/config.yaml` 中定义认证和授权配置，具体配置项如下：

```yaml
authProviderConfig:
  #认证服务器的 URL，用于认证客户端。
  authConnectionUrl: "http://<auth-server-url>/auth"
```

### 2. 构建和打包

通过 Maven 构建并打包插件：

```bash
mvn clean package
```

构建成功后，插件包将生成在 `target/` 目录中，例如 `target/bifromq-auth-provider-plugin-1.0.0.zip`。

### 3. 部署和加载插件

将插件包上传到 BifroMQ 插件目录（假设目录为 `/home/bifromq/plugins`）：

然后启动 BifroMQ 服务，插件将自动加载。

在 BifroMQ 中运行此插件时，需要通过配置文件指定 Auth Provider 的完全限定类名（FQN）。请注意，BifroMQ 一次只允许运行一个 Auth
Provider 实例。

在 BifroMQ 配置文件 `standalone.yml` 中添加以下内容：

```yaml
authProviderFQN: com.mqttsnet.thinglinks.BifromqAuthProviderPluginAuthProvider
```

## 使用说明

### 代码示例

#### 初始化认证上下文

在插件初始化时，通过 `BifromqAuthProviderContext` 加载配置：

```java
public class BifromqAuthProviderPluginAuthProvider {
    private final String authUrl;

    public BifromqAuthProviderPluginAuthProvider(BifromqAuthProviderContext context) {
        this.authUrl = context.getPluginConfig().getAuthProviderConfig().getAuthConnectionUrl();
        // 初始化其他配置项
    }
}
```

#### 认证方法实现

通过 `auth()` 方法进行客户端的认证，使用认证结果控制是否允许连接：

```java
public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData){
        // 构造认证请求，发送至服务器，并解析响应
        }
```

#### 权限检查实现

`checkPermission()` 方法用于检查客户端的发布、订阅权限：

```java
public CompletableFuture<CheckResult> checkPermission(ClientInfo client,MQTTAction action){
        return CompletableFuture.supplyAsync(()->{
        // 根据 action 类型和 topic 进行权限判断
        });
        }
```

## 常见问题

### 如何在不重启 BifroMQ 的情况下更新配置？

可以直接修改 BifroMQ 配置文件 `standalone.yml` 中的配置项，并重启插件实现动态更新。
程序会自动覆盖模块 `conf/config.yaml` 中定义的配置（standalone.yml 优先级高于 config.yaml）

### 如何启用详细日志？

修改 `logback.xml` 中的 `<level>` 标签，将日志级别设置为 `DEBUG`，然后重启插件。

### 认证请求失败怎么办？

请检查 `config.yaml` 中 `authConnectionUrl` 和 `checkUrl` 是否正确配置，并确保认证服务器处于正常运行状态。

## 贡献

欢迎提出 issue 和 pull request 以改进插件功能。如有疑问，请联系项目维护社区 MQTTSNET。

--- 
