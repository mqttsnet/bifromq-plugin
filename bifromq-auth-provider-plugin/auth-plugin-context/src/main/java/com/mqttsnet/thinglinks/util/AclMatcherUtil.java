package com.mqttsnet.thinglinks.util;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mqttsnet.basic.utils.StrPool;
import com.mqttsnet.thinglinks.entity.acl.DeviceAclRule;
import lombok.extern.slf4j.Slf4j;

/**
 * Description:
 * MQTT ACL 主题匹配工具类
 *
 * <p>提供高效的主题匹配功能，支持 MQTT 通配符（+ 和 #）。</p>
 *
 * @author mqttsnet
 * @version 1.0.4
 * @since 2025/6/14
 */
@Slf4j
public class AclMatcherUtil {

    // 使用常量定义通配符和分隔符
    private static final String SINGLE_LEVEL_WILDCARD = StrPool.PLUS;
    private static final String MULTI_LEVEL_WILDCARD = StrPool.HASH;
    private static final String SYSTEM_TOPIC_PREFIX = StrPool.DOLLAR;
    private static final String TOPIC_SEPARATOR = StrPool.SLASH;

    private static final Pattern MULTI_LEVEL_PATTERN = Pattern.compile("^.*$");
    private static final Pattern SYSTEM_TOPIC_PATTERN = Pattern.compile("^\\$[^/]+(?:/[^/]+)*$");
    private static final Pattern NEVER_MATCH_PATTERN = Pattern.compile("a^");


    private static final Executor PATTERN_COMPILATION_EXECUTOR =
            new ThreadPoolExecutor(
                    Runtime.getRuntime().availableProcessors(),
                    Runtime.getRuntime().availableProcessors() * 2,
                    60, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(100_000),
                    new ThreadFactoryBuilder()
                            .setNameFormat("acl-pattern-compile-%d")
                            .setDaemon(true)
                            .build(),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
    private static final AsyncLoadingCache<String, Pattern> PATTERN_CACHE = Caffeine.newBuilder()
            .maximumSize(100_000)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .expireAfterWrite(30, TimeUnit.MINUTES)
            .refreshAfterWrite(5, TimeUnit.MINUTES)
            .executor(PATTERN_COMPILATION_EXECUTOR)
            .recordStats() // 启用统计
            .buildAsync(AclMatcherUtil::loadPattern);


    /**
     * 检查主题是否被允许访问 （默认拒绝）
     *
     * @param topic 待检查的MQTT主题
     * @param rules ACL规则列表（需按优先级排序）
     * @return {@link Boolean} 是否允许访问
     */
    public static Boolean isTopicAllowed(String topic, List<DeviceAclRule> rules) {
        return Optional.ofNullable(topic)
                .filter(StrUtil::isNotBlank)
                .flatMap(t -> Optional.ofNullable(rules)
                        .filter(CollUtil::isNotEmpty)
                        .flatMap(r -> findMatchingRule(t, r)))
                .map(DeviceAclRule::getDecision)
                .orElse(false);
    }

    /**
     * 查找匹配的最高优先级规则
     *
     * @param topic 待检查的主题
     * @param rules ACL规则列表
     * @return 匹配的最高优先级规则（Optional）
     */
    public static Optional<DeviceAclRule> findMatchingRule(String topic, List<DeviceAclRule> rules) {
        if (StrUtil.isBlank(topic) || CollUtil.isEmpty(rules)) {
            return Optional.empty();
        }

        for (DeviceAclRule rule : getSortedRules(rules)) {
            if (isTopicMatch(rule.getTopicPattern(), topic)) {
                // 找到即返回，不再检查后续规则
                return Optional.of(rule);
            }
        }
        return Optional.empty();
    }

    private static List<DeviceAclRule> getSortedRules(List<DeviceAclRule> rules) {
        return rules.stream()
                .filter(Objects::nonNull)
                .filter(rule -> Boolean.TRUE.equals(rule.getEnabled()))
                .sorted(Comparator.comparingInt(DeviceAclRule::getPriority))
                .collect(Collectors.toList());
    }

    /**
     * 严格遵循MQTT规范的主题匹配方法
     * <p>
     * MQTT主题匹配规则:
     * 1. 主题层级分隔符：/
     * 2. 单层通配符：+（匹配一个层级内的任意字符串，但不能为空）
     * 3. 多层通配符：#（匹配任意多个层级，必须位于主题末尾）
     * <p>
     * 特殊规则:
     * - 系统主题（以$开头）需要精确匹配
     * - 空层级需要特殊处理
     *
     * @param pattern 主题模式
     * @param topic   待匹配的主题
     * @return {@link Boolean} 是否匹配
     */
    public static Boolean isTopicMatch(String pattern, String topic) {
        return Optional.ofNullable(pattern)
                .flatMap(p -> Optional.ofNullable(topic)
                        .map(t -> safeIsTopicMatch(p, t)))
                .orElse(false);
    }


    private static Boolean safeIsTopicMatch(String pattern, String topic) {
        // 特殊处理：通配符#匹配所有主题
        if (MULTI_LEVEL_WILDCARD.equals(pattern)) {
            return true;
        }
        try {
            // 异步获取模式，设置超时防止阻塞
            Pattern compiled = PATTERN_CACHE.get(pattern).get(100, TimeUnit.MILLISECONDS);
            return compiled.matcher(topic).matches();
        } catch (Exception e) {
            // 超时或异常时降级为同步编译
            log.warn("Async pattern load failed, falling back to sync: {}", pattern);
            return fallbackMatch(pattern, topic);
        }
    }

    private static Pattern loadPattern(String pattern) {
        try {
            if (MULTI_LEVEL_WILDCARD.equals(pattern)) {
                return MULTI_LEVEL_PATTERN;
            }
            if (pattern.startsWith(SYSTEM_TOPIC_PREFIX)) {
                return SYSTEM_TOPIC_PATTERN.matcher(pattern).matches() ?
                        Pattern.compile(Pattern.quote(pattern)) :
                        NEVER_MATCH_PATTERN;
            }
            return compilePattern(pattern);
        } catch (Exception e) {
            log.error("Pattern compilation failed: {}", pattern, e);
            // 返回一个永远不会匹配的模式
            return NEVER_MATCH_PATTERN;
        }
    }

    private static Boolean fallbackMatch(String pattern, String topic) {
        try {
            // 同步编译并直接匹配（避免缓存）
            Pattern compiled = compilePattern(pattern);
            return compiled.matcher(topic).matches();
        } catch (Exception e) {
            log.error("Fallback pattern compilation failed: {}", pattern, e);
            return false;
        }
    }

    private static Pattern compilePattern(String pattern) {
        String[] levels = pattern.split(TOPIC_SEPARATOR, -1);
        StringBuilder regex = new StringBuilder("^");

        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];

            if (MULTI_LEVEL_WILDCARD.equals(level)) {
                // 确保 # 只在末尾
                if (i != levels.length - 1) {
                    throw new IllegalArgumentException("多层通配符#必须位于主题末尾: " + pattern);
                }
                // 多层匹配
                regex.append(".*");
            } else if (SINGLE_LEVEL_WILDCARD.equals(level)) {
                // + 匹配非空层级
                regex.append("[^/]+");
            } else if (level.startsWith(SYSTEM_TOPIC_PREFIX)) {
                // 系统主题精确匹配，支持子层级
                regex.append(Pattern.quote(level)).append("(?:/[^/]+)*");
            } else {
                // 普通层级精确匹配
                regex.append(Pattern.quote(level));
            }

            // 添加层级分隔符（最后一层除外）
            if (i < levels.length - 1) {
                regex.append(Pattern.quote(TOPIC_SEPARATOR));
            }
        }
        regex.append(SYSTEM_TOPIC_PREFIX);
        return Pattern.compile(regex.toString());
    }


    // 添加缓存统计接口
    public static com.github.benmanes.caffeine.cache.stats.CacheStats getCacheStats() {
        return PATTERN_CACHE.synchronous().stats();
    }

    // 添加缓存大小估算
    public static long getCacheEstimatedSize() {
        return PATTERN_CACHE.synchronous().estimatedSize();
    }

    // 添加缓存清理方法（用于压测间重置）
    public static void cleanCache() {
        PATTERN_CACHE.synchronous().invalidateAll();
    }

}
