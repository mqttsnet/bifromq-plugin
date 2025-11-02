package com.mqttsnet.thinglinks.util;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.mqttsnet.thinglinks.entity.acl.DeviceAclRule;
import com.mqttsnet.thinglinks.entity.device.DeviceInfo;
import lombok.extern.slf4j.Slf4j;

/**
 * ACL主题模式占位符替换工具类
 * <p>
 * 提供安全高效的主题模式占位符替换功能，支持以下占位符：
 * <ul>
 *   <li>${app_id} - 应用ID</li>
 *   <li>${user_name} - 设备的用户名</li>
 *   <li>${device_identification} - 设备唯一标识</li>
 *   <li>${product_identification} - 产品唯一标识</li>
 *   <li>${device_sdk_version} - 设备SDK版本</li>
 * </ul>
 * <p>
 *
 * @author mqttsnet
 * @version 1.0.0
 * @since 2025/9/29
 */
@Slf4j
public class AclTopicPatternPlaceholderReplacer {


    public static final String PLACEHOLDER_APP_ID = "${app_id}";
    public static final String PLACEHOLDER_USER_NAME = "${user_name}";
    public static final String PLACEHOLDER_DEVICE_IDENTIFICATION = "${device_identification}";
    public static final String PLACEHOLDER_PRODUCT_IDENTIFICATION = "${product_identification}";
    public static final String PLACEHOLDER_DEVICE_SDK_VERSION = "${device_sdk_version}";

    private AclTopicPatternPlaceholderReplacer() {
        throw new UnsupportedOperationException("ACL主题模式占位符替换工具类不允许实例化");
    }

    /**
     * 批量替换ACL规则列表中的主题模式占位符
     *
     * @param rules         ACL规则列表
     * @param deviceInfoOpt 设备信息Optional
     */
    public static void replacePlaceholders(List<DeviceAclRule> rules, Optional<DeviceInfo> deviceInfoOpt) {
        if (CollectionUtil.isEmpty(rules)) {
            return;
        }
        DeviceInfo deviceInfo = deviceInfoOpt.orElse(null);
        rules.stream()
                .filter(rule -> StrUtil.isNotBlank(rule.getTopicPattern()))
                .forEach(rule -> replacePlaceholder(rule, deviceInfo));
    }

    /**
     * 替换单个ACL规则中的主题模式占位符
     */
    private static void replacePlaceholder(DeviceAclRule rule, DeviceInfo deviceInfo) {
        if (Objects.isNull(rule)) {
            return;
        }
        String originalPattern = rule.getTopicPattern();
        if (!containsPlaceholders(originalPattern)) {
            return;
        }

        String replacedPattern = replacePlaceholdersInternal(originalPattern, deviceInfo);
        if (!originalPattern.equals(replacedPattern)) {
            rule.setTopicPattern(replacedPattern);
        }
    }

    /**
     * 替换字符串中的占位符（内部方法）
     */
    private static String replacePlaceholdersInternal(String pattern, DeviceInfo deviceInfo) {
        if (StrUtil.isBlank(pattern)) {
            return "";
        }
        if (deviceInfo != null) {
            return pattern
                    .replace(PLACEHOLDER_APP_ID, getSafeValue(deviceInfo.getAppId()))
                    .replace(PLACEHOLDER_USER_NAME, getSafeValue(deviceInfo.getUserName()))
                    .replace(PLACEHOLDER_DEVICE_IDENTIFICATION, getSafeValue(deviceInfo.getDeviceIdentification()))
                    .replace(PLACEHOLDER_PRODUCT_IDENTIFICATION, getSafeValue(deviceInfo.getProductIdentification()))
                    .replace(PLACEHOLDER_DEVICE_SDK_VERSION, getSafeValue(deviceInfo.getDeviceSdkVersion()));
        } else {
            // 设备信息为空时，用空字符串替换所有占位符
            return pattern
                    .replace(PLACEHOLDER_APP_ID, "")
                    .replace(PLACEHOLDER_USER_NAME, "")
                    .replace(PLACEHOLDER_DEVICE_IDENTIFICATION, "")
                    .replace(PLACEHOLDER_PRODUCT_IDENTIFICATION, "")
                    .replace(PLACEHOLDER_DEVICE_SDK_VERSION, "");
        }
    }

    /**
     * 安全获取值，避免null
     */
    private static String getSafeValue(String value) {
        return value != null ? value : "";
    }

    /**
     * 检查是否包含占位符
     */
    private static boolean containsPlaceholders(String pattern) {
        if (pattern == null) {
            return false;
        }

        // 使用短路或，发现一个占位符就立即返回
        return pattern.contains(PLACEHOLDER_APP_ID) ||
                pattern.contains(PLACEHOLDER_USER_NAME) ||
                pattern.contains(PLACEHOLDER_DEVICE_IDENTIFICATION) ||
                pattern.contains(PLACEHOLDER_PRODUCT_IDENTIFICATION) ||
                pattern.contains(PLACEHOLDER_DEVICE_SDK_VERSION);
    }

    /**
     * 替换单个字符串中的占位符（不修改规则对象）
     */
    public static String replacePlaceholders(String pattern, Optional<DeviceInfo> deviceInfoOpt) {
        return replacePlaceholdersInternal(pattern, deviceInfoOpt.orElse(null));
    }
}
