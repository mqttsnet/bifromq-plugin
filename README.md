Bifromq 插件库
=================

## 说明
这是一个Bifromq插件库，用于处理Bifromq与物联网业务系统相关集成。

-[部署使用教程](https://mqttsnet.yuque.com/trgbro/thinglinks-pro/rxzz02p70az2lvb7)

-[Bifromq官方WIKI](https://bifromq.io/zh-Hans/docs/plugin/plugin/)

## 用法
### 1. 引入依赖
在pom.xml中引入依赖
```xml
<dependency>
    <groupId>com.baidu.bifromq</groupId>
    <artifactId>bifromq-plugin-auth-provider</artifactId>
    <version>3.2.1</version>
</dependency>

<dependency>
    <groupId>com.baidu.bifromq</groupId>
    <artifactId>bifromq-plugin-event-collector</artifactId>
    <version>3.2.1</version>
</dependency>

<dependency>
    <groupId>com.baidu.bifromq</groupId>
    <artifactId>bifromq-plugin-setting-provider</artifactId>
    <version>3.2.1</version>
</dependency>

```

### 2. 插件说明

Bifromq 服务端支持版本：3.2.1
注意：请保持插件版本和bifromq服务版本一致

|插件名| 所在位置                                 | 说明                              |
|---|--------------------------------------|---------------------------------|
|AuthProvider| bifromq.plugin.provider.AuthProvider | 用于处理Bifromq的认证插件                |
|EventKafkaProvider| bifromq.plugin.provider.EventKafkaProvider | 用于处理Bifromq的事件采集插件,推送事件消息至Kafka |
|SettingProvider| bifromq.plugin.provider.SettingProvider | 用于处理Bifromq的运行时变更的设置项(Setting) ，可以用于在租户级别动态调整BifroMQ的运行时行为 |



### 3. 插件配置

#### 3.1 打包插件
 使用maven package命令打包插件，打包后的插件位于target目录下，如：bifromq-plugin-1.0.0-SNAPSHOT.jar

#### 3.2 配置插件
将打包好的插件放置于Bifromq的插件目录下，如：/opt/bifromq/plugins/，并在Bifromq的配置文件中配置插件，如：
```bifromq/plugins
注：其他无关插件请不要放置于插件目录下，否则会导致Bifromq启动失败
```

```conf/standalone.yaml
# 插件配置
authProviderFQN: bifromq.plugin.provider.AuthProvider
settingProviderFQN: bifromq.plugin.provider.SettingProvider

