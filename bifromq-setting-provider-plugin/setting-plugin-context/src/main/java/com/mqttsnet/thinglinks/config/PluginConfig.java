package com.mqttsnet.thinglinks.config;

import lombok.Data;

/**
 * PluginConfig 用于封装租户级设置的配置。
 */
@Data
public class PluginConfig {
    /**
     * 启用或禁用 MQTT 版本 3.1 支持
     */
    private Boolean MQTT3Enabled;

    /**
     * 启用或禁用 MQTT 版本 3.1.1 支持
     */
    private Boolean mQTT4Enabled;

    /**
     * 启用或禁用 MQTT 版本 5.0 支持
     */
    private Boolean MQTT5Enabled;

    /**
     * 启用或禁用调试模式
     */
    private Boolean DebugModeEnabled;

    /**
     * 强制连接采用瞬态模式
     */
    private Boolean ForceTransient;

    /**
     * 绕过权限检查错误
     */
    private Boolean ByPassPermCheckError;

    /**
     * 启用或禁用有效载荷格式验证
     */
    private Boolean PayloadFormatValidationEnabled;

    /**
     * 启用或禁用消息保留功能
     */
    private Boolean RetainEnabled;

    /**
     * 启用或禁用通配符订阅
     */
    private Boolean WildcardSubscriptionEnabled;

    /**
     * 启用或禁用订阅标识符
     */
    private Boolean SubscriptionIdentifierEnabled;

    /**
     * 启用或禁用共享订阅
     */
    private Boolean SharedSubscriptionEnabled;

    /**
     * 设置最高 QoS 级别
     */
    private Integer MaximumQoS;

    /**
     * 每个主题级别的最大长度
     */
    private Integer MaxTopicLevelLength;

    /**
     * 主题级别的最大数量
     */
    private Integer MaxTopicLevels;

    /**
     * 主题的最大总长度
     */
    private Integer MaxTopicLength;

    /**
     * 主题别名的最大数量
     */
    private Integer MaxTopicAlias;

    /**
     * 共享订阅组的最大成员数
     */
    private Integer MaxSharedGroupMembers;

    /**
     * 每个收件箱的最大主题过滤器数
     */
    private Integer MaxTopicFiltersPerInbox;

    /**
     * 每个连接每秒发布的最大消息数
     */
    private Integer MsgPubPerSec;

    /**
     * 每个连接每秒接收消息的最大数量
     */
    private Integer ReceivingMaximum;

    /**
     * 每个连接的最大入站带宽（以字节为单位）
     */
    private long InBoundBandWidth;

    /**
     * 每个连接的最大出站带宽（以字节为单位）
     */
    private long OutBoundBandWidth;

    /**
     * 最大用户有效负载大小（以字节为单位）
     */
    private Integer MaxUserPayloadBytes;

    /**
     * 当 QoS 为 1 或 2 时，可以重新发送消息的最大次数
     */
    private Integer MaxResendTimes;

    /**
     * 考虑重新发送消息之前的超时时间（秒）
     */
    private Integer ResendTimeoutSeconds;

    /**
     * 每个订阅的最大主题过滤器数
     */
    private Integer MaxTopicFiltersPerSub;

    /**
     * 最大会话过期时间（以秒为单位）
     */
    private Integer MaxSessionExpirySeconds;

    /**
     * 会话收件箱的最大大小
     */
    private Integer SessionInboxSize;

    /**
     * 是否首先丢弃最旧的 QoS 0 消息
     */
    private Boolean QoS0DropOldest;

    /**
     * 保留消息匹配的限制
     */
    private Integer RetainMessageMatchLimit;
}
