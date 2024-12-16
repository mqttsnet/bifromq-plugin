BifroMQ Plugins
=================

## 说明

欢迎使用 BifroMQ 插件集，这是一个为 BifroMQ 框架设计的插件集合，旨在增强其功能性和可扩展性。此项目包括多个插件，每个插件实现特定的功能，例如认证、设置提供、事件收集等。
用于处理Bifromq与物联网业务系统相关集成。

-[部署使用教程](https://mqttsnet.yuque.com/trgbro/thinglinks-pro/rxzz02p70az2lvb7)

-[Bifromq官方WIKI](https://bifromq.io/zh-Hans/docs/plugin/plugin/)

## 插件概述

### 1. BifroMQ Auth Provider Plugin

用于处理 MQTT 客户端的认证和授权，支持多种认证方式。

- **功能特性**：
    - 认证支持（用户名/密码和证书）
    - ACL 控制（访问控制列表）
    - 动态配置支持

### 2. BifroMQ Setting Provider Plugin

允许在运行时为租户级设置提供自定义值，以动态调整 BifroMQ 的服务行为。

- **功能特性**：
    - 动态设置支持
    - 灵活的配置驱动
    - 日志管理功能

### 3. BifroMQ Event Collector Plugin

用于收集 BifroMQ 执行过程中发生的各种事件，支持事件过滤和处理。

- **功能特性**：
    - 事件收集机制
    - 支持事件过滤逻辑
    - 性能监控和指标记录

## 快速开始

### 1. 环境要求

确保你已安装以下工具：

- Java JDK 17 或更高版本
- Maven 3.6 或更高版本
- BifroMQ 运行时环境

> **注意**：BifroMQ 服务端版本必须与插件版本一致。

### 2. 插件版本

当前支持插件版本为 **3.3.3**。你可以在插件的 `pom.xml` 文件中 bifromq.version 查看当前支持服务端版本。

### 3. 构建插件

每个插件可以单独构建和打包。进入相应插件目录，运行以下命令：

```bash
mvn clean package
```

构建成功后，插件包将生成在对应插件目录的 target/ 目录中。

### 4. 部署插件

将构建的插件包上传到 BifroMQ 插件目录（无需手动解压，程序会自动处理），确保 BifroMQ 配置文件正确指向插件的完全限定类名（详细说明请查阅插件对应的 README
文件）。

## 贡献

欢迎提出 issue 和 pull request 以改进插件功能。如有疑问，请联系项目维护社区 MQTTSNET。

--- 
