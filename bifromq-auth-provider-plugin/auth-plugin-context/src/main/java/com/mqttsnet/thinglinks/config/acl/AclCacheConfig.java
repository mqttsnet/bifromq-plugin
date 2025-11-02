package com.mqttsnet.thinglinks.config.acl;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.mqttsnet.basic.model.cache.CacheKey;
import com.mqttsnet.thinglinks.entity.config.AuthProviderConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Description:
 * ACL 缓存配置
 *
 * @author Sun ShiHuan
 * @version 1.0.4
 * @since 2025/6/17
 */
@Slf4j
public final class AclCacheConfig {
    public static Cache<CacheKey, Boolean> buildCache(
            AuthProviderConfig.AclConfig.CacheConfig aclCacheConfig,
            Executor executor) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .expireAfterWrite(aclCacheConfig.getExpireMinutes(), TimeUnit.MINUTES)
                .maximumSize(aclCacheConfig.getMaxSize())
                .executor(executor)
                .removalListener((key, value, cause) ->
                        log.info("Cache removed: key={}, reason={}", key, cause));

        if (aclCacheConfig.isRecordStats()) {
            builder.recordStats();
        }

        // TODO 处理 CacheLoader
        /*if (config.getRefreshAfterWrite() > 0) {
            builder.refreshAfterWrite(aclCacheConfig.getRefreshAfterWrite(), TimeUnit.MINUTES);
        }*/
        log.info("ACL缓存已启用 - 最大容量: {}, 过期时间: {}分钟, 刷新间隔: {}分钟", aclCacheConfig.getMaxSize(), aclCacheConfig.getExpireMinutes(), aclCacheConfig.getRefreshAfterWrite());
        return builder.build();
    }
}