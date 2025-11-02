package com.mqttsnet.thinglinks.entity.config;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class AuthProviderConfig {
    // 认证服务配置
    private AuthConfig auth;
    // ACL 服务配置
    private AclConfig acl;

    /**
     * 认证服务配置
     */
    @Data
    public static class AuthConfig {
        // 基础服务地址
        private String baseUrl;
        // 客户端认证接口路径
        private String clientAuthEndpoint;

        /**
         * 获取完整认证地址
         */
        public String getClientAuthUrl() {
            return baseUrl + clientAuthEndpoint;
        }
    }

    /**
     * ACL 服务配置
     */
    @Data
    public static class AclConfig {
        // 是否启用 ACL 检查
        private boolean enabled = true;
        // 基础服务地址
        private String baseUrl;
        // ACL 检查接口路径
        private String aclCheckEndpoint;

        // 租户白名单（开启后，只有白名单中的租户ID不会进行ACL校验）
        private List<String> tenantWhitelist = new ArrayList<>();

        // ACL 缓存配置
        private CacheConfig cache = new CacheConfig();

        /**
         * 获取完整 ACL 检查地址
         */
        public String getAclCheckUrl() {
            return baseUrl + aclCheckEndpoint;
        }

        @Data
        public static class CacheConfig {
            private int maxSize = 100_0000;          // 默认100万条
            private int expireMinutes = 10;          // 默认10分钟过期
            private boolean recordStats = true;     // 是否记录统计信息
            private int refreshAfterWrite = 2;      // 写后2分钟刷新（单位：分钟）
        }
    }
}

