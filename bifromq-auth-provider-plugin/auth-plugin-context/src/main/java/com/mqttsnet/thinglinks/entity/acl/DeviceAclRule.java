package com.mqttsnet.thinglinks.entity.acl;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

import cn.hutool.core.map.MapUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * <p>
 * 设备访问控制(ACL)规则
 * </p>
 *
 * @author mqttsnet
 * @date 2025-06-11 19:57:46
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@Builder
public class DeviceAclRule implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Map<String, Object> echoMap = MapUtil.newHashMap();

    private Long id;

    /**
     * 规则名称
     */
    private String ruleName;

    /**
     * 设备标识
     */
    private String deviceIdentification;
    /**
     * 动作类型((0:全部、1:发布、2:订阅、3:取消订阅))
     */
    private Integer actionType;
    /**
     * 规则优先级(0-1000,值越小优先级越高)
     */
    private Integer priority;
    /**
     * MQTT主题模式(支持通配符)
     */
    private String topicPattern;
    /**
     * IP白名单地址(多个用逗号分隔)
     */
    private String ipWhitelist;
    /**
     * 决策(0:拒绝、1:允许)
     */
    private Boolean decision;
    /**
     * 是否启用
     */
    private Boolean enabled;
    /**
     * 产品标识
     */
    private String productIdentification;
    /**
     * 规则级别(0:产品级、1:设备级)
     */
    private Integer ruleLevel;


}
