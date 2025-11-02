package com.mqttsnet.thinglinks.constant;

/**
 * 公共常量类，定义系统中广泛使用的常量字段
 *
 * @author mqttsnet
 */
public class CommonConstants {

    /**
     * MQTT协议类型
     */
    public static final String MQTT_PROTOCOL_TYPE = "MQTT";


    /**
     * 成功标识
     */
    public static final String SUCCESS = "success";

    /**
     * 失败标识
     */
    public static final String FAILURE = "failure";


    /**
     * 密码字段名，用于JSON消息中标识密码字段
     */
    public static final String PASSWORD = "password";


    /**
     * 用户名字段名，用于JSON消息中标识用户名字段
     */
    public static final String USERNAME = "username";


    /**
     * 认证模式字段名，用于JSON消息中标识认证模式
     */
    public static final String AUTH_MODE = "authMode";

    /**
     * 客户端证书字段名，用于JSON消息中标识客户端证书字段
     */
    public static final String CLIENT_CERTIFICATE = "clientCertificate";

    /**
     * 认证结果字段名，用于标识认证是否成功
     */
    public static final String CERTIFICATION_RESULT = "certificationResult";

    /**
     * 事件类型字段名，用于JSON消息中标识事件类型
     * 例如：MQTT事件、WebSocket事件等各类事件的类型标识
     */
    public static final String EVENT_TYPE = "eventType";

    /**
     * 事件时间字段名，用于JSON消息中标识事件发生的时间戳
     * 通常是毫秒级别的时间戳，用于事件的时间排序和查询
     */
    public static final String EVENT_TIME = "eventTime";

    /**
     * 事件时间字符串字段名，用于JSON消息中标识事件发生的时间字符串
     * 通常是格式化后的时间字符串，用于人类可读的时间显示
     */
    public static final String EVENT_TIME_STR = "eventTimeStr";

    /**
     * 操作类型字段名，用于JSON消息中标识操作类型
     * 例如：发布、订阅、取消订阅等操作类型的标识
     */
    public static final String ACTION_TYPE = "actionType";

    /**
     * 协议类型字段名，用于JSON消息中标识MQTT协议类型
     * 例如：MQTT、MQTT-SN等不同协议的类型标识
     */
    public static final String PROTOCOL_TYPE = "protocolType";

    /**
     * 租户ID字段名，用于JSON消息中标识数据所属租户
     * 在多租户系统中用于数据隔离和权限控制
     */
    public static final String TENANT_ID = "tenantId";


    /**
     * 客户端ID字段名，用于标识MQTT客户端的唯一标识符
     * 在连接、断开连接等事件中用于识别特定客户端
     */
    public static final String CLIENT_ID = "clientId";

    /**
     * 客户端标识符字段名，用于标识MQTT客户端的唯一标识符
     * 在连接、断开连接等事件中用于识别特定客户端
     */
    public static final String CLIENT_IDENTIFIER = "clientIdentifier";

    /**
     * 用户ID字段名，用于标识用户的唯一标识符
     * 在认证、授权等场景中用于识别特定用户
     */
    public static final String USER_ID = "userId";


    /**
     * 版本号
     */
    public static final String VERSION = "version";


    /**
     * ACL规则字段名，用于标识ACL（访问控制列表）规则
     * 在授权决策中用于判断客户端是否有权限执行特定操作
     */
    public static final String ACL_RULE = "aclRule";

    /**
     * 业务平台返回的ACL规则结果字段名，用于标识ACL（访问控制列表）规则的执行结果
     */
    public static final String ACL_RULE_LIST_RESULT = "aclRuleListResult";


    /**
     * 设备信息字段名，用于标识设备的相关信息
     * 在设备连接、数据交互等场景中用于传递设备相关数据
     */
    public static final String DEVICE_INFO = "deviceInfo";

    /**
     * 业务平台返回的设备信息结果字段名，用于标识设备的相关信息
     * 在设备连接、数据交互等场景中用于传递设备相关数据
     */
    public static final String DEVICE_INFO_RESULT = "deviceInfoResult";

    /**
     * 主题字段名，用于标识MQTT消息的主题
     * 在发布、订阅等操作中用于指定消息的目标主题
     */
    public static final String TOPIC = "topic";


}