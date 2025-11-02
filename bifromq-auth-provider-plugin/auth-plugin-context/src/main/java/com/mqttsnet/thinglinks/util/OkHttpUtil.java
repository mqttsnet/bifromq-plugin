package com.mqttsnet.thinglinks.util;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.mqttsnet.thinglinks.config.interceptor.SmartRetryInterceptor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * Description:
 * OkHttp 工具类，封装常见的 HTTP 请求方法
 * <p>支持同步/异步请求、文件上传下载、自定义超时、全局拦截器、自动重试等生产级特性</p>
 * <p>使用泛型支持任意响应类型，通过 ResponseConverter 接口实现响应转换</p>
 *
 * @author mqttsnet
 * @version 1.0.4
 * @since 2025/6/9
 */
@Slf4j
public class OkHttpUtil {

    // 默认重试次数和间隔
    private static final int DEFAULT_RETRY_COUNT = 3;
    private static final long DEFAULT_RETRY_INTERVAL = 1000L; // 重试间隔1秒
    private static final OkHttpClient DEFAULT_CLIENT;
    private static final Map<String, String> GLOBAL_HEADERS = new ConcurrentHashMap<>();
    private static volatile OkHttpClient customClient;

    static {
        ConnectionPool connectionPool = new ConnectionPool(50, 5, TimeUnit.MINUTES);
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(1000);
        dispatcher.setMaxRequestsPerHost(50);

        DEFAULT_CLIENT = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)   // 连接超时10秒
                .readTimeout(5, TimeUnit.SECONDS)      // 读取超时5秒
                .writeTimeout(5, TimeUnit.SECONDS)     // 写入超时5秒
                .retryOnConnectionFailure(true)
                .connectionPool(connectionPool)         // 连接池配置
                .dispatcher(dispatcher)                 // 调度器配置
                .addInterceptor(new SmartRetryInterceptor(
                        DEFAULT_RETRY_COUNT,    // 最大重试次数
                        DEFAULT_RETRY_INTERVAL  // 基础延迟时间(毫秒)
                ))
                .build();

        log.info("OkHttp工具类初始化完成 - 最大重试次数: {}, 连接池: {}/{} (活跃/空闲)",
                DEFAULT_RETRY_COUNT, connectionPool.connectionCount(), connectionPool.idleConnectionCount());
    }

    /**
     * 响应转换器接口，用于将响应体转换为指定类型
     *
     * @param <T> 目标类型
     */
    public interface ResponseConverter<T> {
        /**
         * 将响应体转换为指定类型
         *
         * @param responseBody 响应体字符串
         * @return 转换后的对象
         */
        T convert(String responseBody);
    }

    /**
     * 配置全局请求头（所有请求自动携带）
     *
     * @param key   请求头名称
     * @param value 请求头值
     */
    public static void addGlobalHeader(String key, String value) {
        if (key != null && value != null) {
            GLOBAL_HEADERS.put(key, value);
            log.debug("添加全局请求头 - {}: {}", key, value);
        }
    }

    /**
     * 移除全局请求头
     *
     * @param key 要移除的请求头名称
     */
    public static void removeGlobalHeader(String key) {
        if (key != null) {
            String removed = GLOBAL_HEADERS.remove(key);
            log.debug("移除全局请求头 - {}: {}", key, removed);
        }
    }

    /**
     * 设置自定义 OkHttpClient 实例
     *
     * @param client 自定义的 OkHttpClient 实例
     */
    public static void setCustomClient(OkHttpClient client) {
        customClient = client;
        log.info("设置自定义OkHttpClient实例 - 客户端: {}", client != null ? client.toString() : "null");
    }

    // 获取当前使用的客户端实例
    private static OkHttpClient getClient() {
        return customClient != null ? customClient : DEFAULT_CLIENT;
    }

    // 应用全局请求头到请求构建器
    private static Request.Builder applyGlobalHeaders(Request.Builder builder) {
        GLOBAL_HEADERS.forEach((key, value) -> {
            if (key != null && value != null) {
                builder.addHeader(key, value);
                log.trace("应用全局请求头 - {}: {}", key, value);
            }
        });
        return builder;
    }

    /**
     * 发送同步 GET 请求
     *
     * @param url       请求的 URL
     * @param converter 响应转换器
     * @return 响应结果 Optional 包装
     * @throws IOException 网络请求异常
     */
    public static <T> Optional<T> sendGetRequest(String url, ResponseConverter<T> converter) throws IOException {
        return sendGetRequest(url, null, null, converter);
    }

    /**
     * 发送带参数的同步 GET 请求
     *
     * @param url       请求的 URL
     * @param params    请求参数
     * @param converter 响应转换器
     * @return 响应结果 Optional 包装
     * @throws IOException 网络请求异常
     */
    public static <T> Optional<T> sendGetRequest(String url, Map<String, String> params, ResponseConverter<T> converter) throws IOException {
        return sendGetRequest(url, params, null, converter);
    }

    /**
     * 发送带请求头的同步 GET 请求
     *
     * @param url       请求的 URL
     * @param params    请求参数
     * @param headers   自定义请求头
     * @param converter 响应转换器
     * @return 响应结果 Optional 包装
     * @throws IOException 网络请求异常
     */
    public static <T> Optional<T> sendGetRequest(String url, Map<String, String> params, Map<String, String> headers, ResponseConverter<T> converter) throws IOException {
        // 参数校验
        if (url == null || url.trim().isEmpty()) {
            log.warn("GET 请求 URL 不能为空");
            return Optional.empty();
        }

        HttpUrl httpUrl = HttpUrl.parse(url);
        if (httpUrl == null) {
            log.error("无效的 URL: {}", url);
            return Optional.empty();
        }

        HttpUrl.Builder urlBuilder = httpUrl.newBuilder();
        if (params != null) {
            params.forEach((key, value) -> {
                if (key != null && value != null) {
                    urlBuilder.addQueryParameter(key, value);
                }
            });
        }

        Request.Builder builder = new Request.Builder().url(urlBuilder.build());
        applyGlobalHeaders(builder);
        if (headers != null) {
            headers.forEach((key, value) -> {
                if (key != null && value != null) {
                    builder.addHeader(key, value);
                }
            });
        }

        try (Response response = getClient().newCall(builder.build()).execute()) {
            return handleResponse(response, converter);
        } catch (IOException e) {
            log.error("GET 请求失败: {} | 错误: {}", url, e.getMessage());
            throw e;
        }
    }

    /**
     * 发送 JSON 格式的同步 POST 请求
     *
     * @param url       请求的 URL
     * @param jsonBody  JSON 格式的请求体
     * @param converter 响应转换器
     * @return 响应结果 Optional 包装
     * @throws IOException 网络请求异常
     */
    public static <T> Optional<T> sendPostRequest(String url, String jsonBody, ResponseConverter<T> converter) throws IOException {
        return sendPostRequest(url, jsonBody, null, converter);
    }

    /**
     * 发送带请求头的 JSON 格式同步 POST 请求
     *
     * @param url       请求的 URL
     * @param jsonBody  JSON 格式的请求体
     * @param headers   自定义请求头
     * @param converter 响应转换器
     * @return 响应结果 Optional 包装
     * @throws IOException 网络请求异常
     */
    public static <T> Optional<T> sendPostRequest(String url, String jsonBody, Map<String, String> headers, ResponseConverter<T> converter) throws IOException {
        long startTime = System.currentTimeMillis();

        if (url == null || url.trim().isEmpty()) {
            log.warn("POST请求失败 - URL为空");
            return Optional.empty();
        }
        if (jsonBody == null) {
            log.warn("POST请求失败 - 空JSON请求体, URL: {}", url);
            return Optional.empty();
        }

        MediaType JSON = MediaType.get("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(jsonBody, JSON);

        Request.Builder builder = new Request.Builder()
                .url(url)
                .post(body);
        applyGlobalHeaders(builder);

        if (headers != null) {
            headers.forEach((key, value) -> {
                if (key != null && value != null) {
                    builder.addHeader(key, value);
                    log.trace("添加请求头 - {}: {}", key, value);
                }
            });
        }

        Request request = builder.build();
        log.debug("发送POST请求 - URL: {}, 请求头: {}个, 请求体大小: {}字节",
                url,
                headers != null ? headers.size() : 0,
                jsonBody.length());

        try (Response response = getClient().newCall(request).execute()) {
            long elapsed = System.currentTimeMillis() - startTime;
            log.debug("POST请求完成 - URL: {}, 状态码: {}, 耗时: {}ms, 响应体大小: {}字节",
                    url,
                    response.code(),
                    elapsed,
                    response.body() != null ? response.body().contentLength() : 0);

            return handleResponse(response, converter);
        } catch (IOException e) {
            long elapsed = System.currentTimeMillis() - startTime;
            log.error("POST请求失败 - URL: {}, 错误: {}, 耗时: {}ms, 请求体大小: {}字节",
                    url,
                    e.getMessage(),
                    elapsed,
                    jsonBody.length(),
                    e);
            throw e;
        }
    }

    /**
     * 发送 POST 请求并返回状态码
     *
     * @param url      请求的 URL
     * @param jsonBody JSON 格式的请求体
     * @param headers  自定义请求头
     * @return HTTP 状态码
     * @throws IOException 网络请求异常
     */
    public static int sendPostRequestForStatus(String url, String jsonBody, Map<String, String> headers) throws IOException {
        long startTime = System.currentTimeMillis();

        if (url == null || url.trim().isEmpty()) {
            log.warn("POST请求(仅状态)失败 - URL为空");
            return -1;
        }

        MediaType JSON = MediaType.get("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(jsonBody, JSON);

        Request.Builder builder = new Request.Builder()
                .url(url)
                .post(body);
        applyGlobalHeaders(builder);

        if (headers != null) {
            headers.forEach((key, value) -> {
                if (key != null && value != null) {
                    builder.addHeader(key, value);
                    log.trace("添加请求头 - {}: {}", key, value);
                }
            });
        }

        Request request = builder.build();
        log.debug("发送POST请求(仅状态) - URL: {}, 请求头: {}个, 请求体大小: {}字节",
                url,
                headers != null ? headers.size() : 0,
                jsonBody.length());

        try (Response response = getClient().newCall(request).execute()) {
            int status = response.code();
            long elapsed = System.currentTimeMillis() - startTime;
            log.debug("POST请求(仅状态)完成 - URL: {}, 状态码: {}, 耗时: {}ms",
                    url, status, elapsed);
            return status;
        } catch (IOException e) {
            long elapsed = System.currentTimeMillis() - startTime;
            log.error("POST请求(仅状态)失败 - URL: {}, 错误: {}, 耗时: {}ms",
                    url, e.getMessage(), elapsed, e);
            throw e;
        }
    }

    /**
     * 统一处理响应，使用转换器转换为指定类型
     *
     * @param response  HTTP 响应对象
     * @param converter 响应转换器
     * @return Optional 包装的转换结果
     * @throws IOException 读取响应体异常
     */
    private static <T> Optional<T> handleResponse(Response response, ResponseConverter<T> converter) throws IOException {
        if (response == null) {
            log.warn("处理响应失败 - 响应对象为null");
            return Optional.empty();
        }

        if (!response.isSuccessful()) {
            log.warn("请求失败 - 状态码: {}, 响应头: {}", response.code(), response.headers());
            return Optional.empty();
        }

        if (response.body() == null) {
            log.warn("空响应体 - 状态码: {}", response.code());
            return Optional.empty();
        }

        try {
            String responseBody = response.body().string();
            if (responseBody == null || responseBody.isEmpty()) {
                log.warn("空响应体内容 - 状态码: {}", response.code());
                return Optional.empty();
            }

            if (converter == null) {
                log.error("响应转换失败 - 转换器为null");
                return Optional.empty();
            }

            T result = converter.convert(responseBody);
            log.trace("响应转换成功 - 响应体: {}", responseBody);
            return Optional.ofNullable(result);
        } catch (Exception e) {
            log.error("响应处理失败 - 状态码: {}, 错误: {}",
                    response.code(), e.getMessage(), e);
            return Optional.empty();
        }
    }

}
