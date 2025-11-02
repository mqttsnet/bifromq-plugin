package com.mqttsnet.thinglinks.entity.device;

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
 * 设备档案信息
 * </p>
 *
 * @author mqttsnet
 * @date 2025-09-14 19:39:59
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@Builder
public class DeviceInfo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Map<String, Object> echoMap = MapUtil.newHashMap();

    /**
     * 客户端标识
     */
    private String clientId;
    /**
     * 用户名
     */
    private String userName;
    /**
     * 应用ID
     */
    private String appId;
    /**
     * 设备标识
     */
    private String deviceIdentification;
    /**
     * 产品标识
     */
    private String productIdentification;
    /**
     * sdk版本
     */
    private String deviceSdkVersion;
}
