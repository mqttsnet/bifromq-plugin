/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.mqttsnet.thinglinks;

import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.*;
import com.baidu.bifromq.type.ClientInfo;
import com.mqttsnet.thinglinks.config.PluginConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.pf4j.Extension;
import org.springframework.http.HttpStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * -----------------------------------------------------------------------------
 * File Name: BifromqAuthProviderPluginAuthProvider
 * -----------------------------------------------------------------------------
 * Description:
 * <a href="https://bifromq.io/zh-Hans/docs/plugin/auth_provider/">...</a>
 * Auth Provider插件旨在为BifroMQ运行时提供验证MQTT客户端连接和授权发布/订阅消息主题的能力
 * 认证提供者插件，用于支持 MQTT3 和 MQTT5 客户端的认证。
 * 支持基于 HTTP 请求的客户端身份验证，能够解析 ACL（访问控制列表）信息并返回给客户端。
 * 通过 {@link BifromqAuthProviderContext} 获取插件的配置信息。
 * <p>
 * 1. 实现IAuthProvider接口
 * 2. 通过@Extension注解标记为插件
 * 3. 实现auth方法，调用ThingLinks 的认证接口验证客户端连接
 * 4. 实现check方法，验证客户端是否有权限执行指定的操作
 * <p>
 * -----------------------------------------------------------------------------
 *
 * @author xiaonannet
 * @version 1.0
 * -----------------------------------------------------------------------------
 * Revision History:
 * Date         Author          Version     Description
 * --------      --------     -------   --------------------
 * 2024/2/23       xiaonannet        1.0        Initial creation
 * -----------------------------------------------------------------------------
 * @email
 * @date 2024/2/23 15:36
 */
@Slf4j
@Extension
public final class BifromqAuthProviderPluginAuthProvider implements IAuthProvider {

    private final String clientConnectionUrl;
    private final ThreadPoolExecutor executor;

    /**
     * 构造函数，通过 {@link BifromqAuthProviderContext} 初始化配置。
     *
     * @param context {@link BifromqAuthProviderContext} 认证插件的上下文，包含配置信息。
     */
    public BifromqAuthProviderPluginAuthProvider(BifromqAuthProviderContext context) {
        PluginConfig pluginConfig = context.getPluginConfig();  // 通过 context 获取配置
        this.clientConnectionUrl = pluginConfig.getAuthProviderConfig().getAuthConnectionUrl();
        log.info("AuthProvider initialized with URL: {}", clientConnectionUrl);

        int corePoolSize = Runtime.getRuntime().availableProcessors() * 10;
        int maximumPoolSize = corePoolSize * 20;
        long keepAliveTime = 60L;
        TimeUnit unit = TimeUnit.SECONDS;
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();

        this.executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }


    /**
     * MQTT3协议的认证方法，验证客户端连接请求。
     *
     * @param authData {@link MQTT3AuthData} 包含MQTT3认证数据
     * @return {@link CompletableFuture} 包含MQTT3认证结果 {@link MQTT3AuthResult}
     */
    @Override
    public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
        String clientId = authData.getClientId();
        String password = authData.getPassword().toStringUtf8();
        String username = authData.getUsername();
        String cert = authData.getCert().toStringUtf8();
        log.info("MQTT3 - Authenticating client - clientId: {}, username: {}, cert: {}", clientId, username, cert);

        return CompletableFuture.supplyAsync(() -> clientConnectionAuthentication(clientId, password, username, cert), executor)
                .thenApply(this::handleMQTT3AuthenticationResponse);
    }

    /**
     * MQTT5协议的认证方法，验证客户端连接请求。
     *
     * @param authData {@link MQTT5AuthData} 包含MQTT5认证数据
     * @return {@link CompletableFuture} 包含MQTT5认证结果 {@link MQTT5AuthResult}
     */
    @Override
    public CompletableFuture<MQTT5AuthResult> auth(MQTT5AuthData authData) {
        String clientId = authData.getClientId();
        String password = authData.getPassword().toStringUtf8();
        String username = authData.getUsername();
        String cert = authData.getCert().toStringUtf8();
        log.info("MQTT5 - Authenticating client - clientId: {}, username: {}, cert: {}", clientId, username, cert);

        return CompletableFuture.supplyAsync(() -> clientConnectionAuthentication(clientId, password, username, cert), executor)
                .thenApply(this::handleMQTT5AuthenticationResponse);
    }

    /**
     * 通过向远程认证服务器发送POST请求执行客户端认证。
     *
     * @param clientId 客户端ID
     * @param password 客户端密码
     * @param username 用户名
     * @param cert     SSL证书信息
     * @return {@link HttpResponse} 认证服务器的响应
     */
    private HttpResponse clientConnectionAuthentication(String clientId, String password, String username, String cert) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("clientIdentifier", clientId);
        jsonObject.put("password", password);
        jsonObject.put("username", username);
        jsonObject.put("protocolType", "MQTT");

        if (StringUtils.isNotBlank(cert)) {
            jsonObject.put("authMode", 1);
            jsonObject.put("clientCertificate", cert);
        } else {
            jsonObject.put("authMode", 0);
        }

        return HttpUtil.createPost(clientConnectionUrl)
                .body(jsonObject.toJSONString())
                .execute();
    }

    /**
     * 处理MQTT3的认证响应，根据认证结果返回MQTT3的认证结果。
     *
     * @param response {@link HttpResponse} 认证服务器的响应
     * @return {@link MQTT3AuthResult} MQTT3认证结果
     */
    private MQTT3AuthResult handleMQTT3AuthenticationResponse(HttpResponse response) {
        int statusCode = response.getStatus();
        String responseBody = response.body();
        log.info("MQTT3 Authentication response - statusCode: {}, responseBody: {}", statusCode, responseBody);

        if (statusCode == HttpStatus.OK.value()) {
            return parseAuthResponseForMQTT3(responseBody);
        } else {
            return createMQTT3RejectResponse("Authentication failed with status: " + statusCode);
        }
    }

    /**
     * 处理MQTT5的认证响应，根据认证结果返回MQTT5的认证结果。
     *
     * @param response {@link HttpResponse} 认证服务器的响应
     * @return {@link MQTT5AuthResult} MQTT5认证结果
     */
    private MQTT5AuthResult handleMQTT5AuthenticationResponse(HttpResponse response) {
        int statusCode = response.getStatus();
        String responseBody = response.body();
        log.info("MQTT5 Authentication response - statusCode: {}, responseBody: {}", statusCode, responseBody);

        if (statusCode == HttpStatus.OK.value()) {
            return parseAuthResponseForMQTT5(responseBody);
        } else {
            return createMQTT5RejectResponse("Authentication failed with status: " + statusCode);
        }
    }

    /**
     * 解析认证响应以生成MQTT3的认证结果。
     *
     * @param responseBody 认证响应的内容
     * @return {@link MQTT3AuthResult} MQTT3认证结果
     */
    private MQTT3AuthResult parseAuthResponseForMQTT3(String responseBody) {
        JSONObject responseJson = JSON.parseObject(responseBody);
        boolean certificationResult = responseJson.getBoolean("certificationResult");

        if (certificationResult) {
            Ok.Builder okBuilder = buildOkResponse(responseJson);
            return MQTT3AuthResult.newBuilder().setOk(okBuilder.build()).build();
        } else {
            return createMQTT3RejectResponse("Certification result failed");
        }
    }

    /**
     * 解析认证响应以生成MQTT5的认证结果。
     *
     * @param responseBody 认证响应的内容
     * @return {@link MQTT5AuthResult} MQTT5认证结果
     */
    private MQTT5AuthResult parseAuthResponseForMQTT5(String responseBody) {
        JSONObject responseJson = JSON.parseObject(responseBody);
        boolean certificationResult = responseJson.getBoolean("certificationResult");

        if (certificationResult) {
            Ok.Builder okBuilder = buildOkResponse(responseJson);
            // 创建 Success 构建器并填充属性
            Success.Builder successBuilder = Success.newBuilder()
                    .setTenantId(okBuilder.getTenantId())
                    .setUserId(okBuilder.getUserId())
                    .putAllAttrs(okBuilder.getAttrsMap());

            return MQTT5AuthResult.newBuilder()
                    .setSuccess(successBuilder.build())
                    .build();
        } else {
            return createMQTT5RejectResponse("Certification result failed");
        }
    }

    /**
     * 构建Ok认证结果，包括tenantId、userId和自定义ACL属性。
     *
     * @param responseJson 认证服务器返回的JSON对象
     * @return {@link Ok.Builder} 包含认证信息的Ok构建器
     */
    private Ok.Builder buildOkResponse(JSONObject responseJson) {
        Optional<JSONObject> deviceResultJson = Optional.ofNullable(responseJson.getJSONObject("deviceResult"));
        String clientId = deviceResultJson.flatMap(json -> Optional.ofNullable(json.getString("clientId"))).orElse("");
        String tenantId = Optional.ofNullable(responseJson.getString("tenantId")).orElse("");

        // TODO 认证接口返回ACL 控制参数（ ACL 直接嵌入在令牌中。此信息将在发布/订阅期间用于访问控制。在当前工作流中，每个会话的 ClientInfo（在成功连接后填充）仅包含有限的保留元数据。）
        Map<String, String> attrsMap = new HashMap<>();
        String aclData = Optional.ofNullable(responseJson.getString("acl")).orElse("");
        if (!aclData.isEmpty()) {
            attrsMap.put("acl", aclData);
        }

        log.info("Authentication successful - clientId: {}, tenantId: {}, ACL: {}", clientId, tenantId, aclData);

        return Ok.newBuilder()
                .setTenantId(tenantId)
                .setUserId(clientId)
                .putAllAttrs(attrsMap);
    }

    /**
     * 创建MQTT3的拒绝认证响应。
     *
     * @param reason 拒绝原因
     * @return {@link MQTT3AuthResult} 拒绝认证结果
     */
    private MQTT3AuthResult createMQTT3RejectResponse(String reason) {
        log.info("MQTT3 Authentication rejected - reason: {}", reason);
        return MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                        .setCode(Reject.Code.NotAuthorized)
                        .setReason(reason)
                        .build())
                .build();
    }

    /**
     * 创建MQTT5的拒绝认证响应。
     *
     * @param reason 拒绝原因
     * @return {@link MQTT5AuthResult} 拒绝认证结果
     */
    private MQTT5AuthResult createMQTT5RejectResponse(String reason) {
        log.info("MQTT5 Authentication rejected - reason: {}", reason);
        return MQTT5AuthResult.newBuilder()
                .setFailed(Failed.newBuilder()
                        .setCode(Failed.Code.NotAuthorized)
                        .setReason(reason)
                        .build())
                .build();
    }


    /**
     * 执行 MQTT5 扩展认证。根据输入的认证数据决定是成功、继续或失败。
     * TODO 暂时先不启用
     *
     * @param authData 包含认证数据的 {@link MQTT5ExtendedAuthData} 对象
     * @return {@link CompletableFuture<MQTT5ExtendedAuthResult>} 包含认证结果
     */
//    @Override
    public CompletableFuture<MQTT5ExtendedAuthResult> extendedAuth(MQTT5ExtendedAuthData authData) {
        // 异步处理认证请求
        return CompletableFuture.supplyAsync(() -> {
            // 检查认证数据类型
            switch (authData.getTypeCase()) {
                case INITIAL -> {
                    // 处理初始认证请求
                    return processInitialAuth(authData.getInitial());
                }
                case AUTH -> {
                    // 处理继续认证请求
                    return processContinueAuth(authData.getAuth());
                }
                default -> {
                    log.error("Received unknown auth data type: {}", authData.getTypeCase());
                    return createMQTT5ExtendedRejectResponse("Unsupported auth data type");
                }
            }
        });
    }

    /**
     * 处理初始认证请求，返回继续认证或拒绝认证的结果。
     *
     * @param initialData 初始认证数据
     * @return {@link MQTT5ExtendedAuthResult} 包含继续认证或拒绝的认证结果
     */
    private MQTT5ExtendedAuthResult processInitialAuth(MQTT5ExtendedAuthData.Initial initialData) {
        log.info("Processing initial auth with data: {}", initialData);

        // 验证初始认证数据并决定是否继续认证
        if (isValidInitialData(initialData)) {
            // 构建继续认证的响应
            return MQTT5ExtendedAuthResult.newBuilder()
                    .setSuccess(Success.newBuilder()
                            .setTenantId("thinglinks")
                            .setUserId("User456")
                            .putAttrs("role", "user")
                            .build())
                    .build();
        } else {
            // 认证失败，返回拒绝响应
            return createMQTT5ExtendedRejectResponse("Initial authentication failed");
        }
    }


    private boolean isValidInitialData(MQTT5ExtendedAuthData.Initial initialData) {
        // 验证初始认证数据的逻辑
        return true;
    }

    private boolean isValidAuthData(MQTT5ExtendedAuthData.Auth authData) {
        // 验证继续认证数据的逻辑
        return true;
    }


    private MQTT5ExtendedAuthResult processContinueAuth(MQTT5ExtendedAuthData.Auth authData) {
        log.info("Processing continue auth with data: {}", authData);

        // 根据继续认证数据进行实际的认证操作
        if (isValidAuthData(authData)) {
            return MQTT5ExtendedAuthResult.newBuilder()
                    .setSuccess(Success.newBuilder()
                            .setTenantId("thinglinks")
                            .setUserId("User456")
                            .putAttrs("role", "user")
                            .build())
                    .build();
        } else {
            return createMQTT5ExtendedRejectResponse("Continue authentication failed");
        }
    }

    private MQTT5ExtendedAuthResult createMQTT5ExtendedRejectResponse(String reason) {
        log.info("MQTT5 Extended Auth rejected - reason: {}", reason);
        return MQTT5ExtendedAuthResult.newBuilder()
                .setFailed(Failed.newBuilder()
                        .setCode(Failed.Code.NotAuthorized)
                        .setReason(reason)
                        .build())
                .build();
    }


    @Override
    public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
//        return CompletableFuture.failedFuture(new UnsupportedOperationException("Unimplemented"));
        return CompletableFuture.completedFuture(true);
    }

    /**
     * 检查客户端的权限，根据操作类型（发布、订阅、退订）决定是否允许操作。
     *
     * @param client 包含客户端信息的 {@link ClientInfo} 对象
     * @param action 包含操作信息的 {@link MQTTAction} 对象
     * @return {@link CompletableFuture<CheckResult>} 包含权限检查结果
     */
    @Override
    public CompletableFuture<CheckResult> checkPermission(ClientInfo client, MQTTAction action) {
        return CompletableFuture.supplyAsync(() -> {
            String checkAction = null;
            String topic = null;

            // 确定动作类型（发布、订阅、退订）并提取相应的主题
            if (action.hasPub()) {
                checkAction = "Pub";
                topic = action.getPub().getTopic();
            } else if (action.hasSub()) {
                checkAction = "Sub";
                topic = action.getSub().getTopicFilter();
            } else if (action.hasUnsub()) {
                checkAction = "Sub";
                topic = action.getUnsub().getTopicFilter();
            }

            // TODO 调用业务系统API查看是否有动作权限，目前是全部都允许

            // 构建 HTTP POST 请求
            /*HttpPost post = new HttpPost(ConfigUtils.getAuthProviderConfig().getDevice().getCheckUrl());
            StringEntity entity = new StringEntity(String.format("{\"username\":\"%s\",\"topic\":\"%s\", \"action\": \"%s\"}",
                    client.getUserId(), topic, checkAction), "UTF-8");
            post.setEntity(entity);
            post.setHeader("Content-Type", "application/json");

            try (CloseableHttpResponse response = httpClient.execute(post)) {
                if (parseAuthResult(response.getEntity())) {
                    // 权限检查通过
                    return CheckResult.newBuilder().setOk(Ok.newBuilder().build()).build();
                } else {
                    // 权限检查未通过
                    return createRejectCheckResult("Permission denied");
                }
            } catch (Exception e) {
                log.warn("Failed to check permission for user: {}, action: {}, topic: {}", client.getUserId(), checkAction, topic, e);
                // 返回错误结果
                return createRejectCheckResult("Error during permission check");
            }*/

            // 默认放行
            return CheckResult.newBuilder().setGranted(Granted.getDefaultInstance()).build();
        });
    }

    /**
     * 解析权限检查结果。
     *
     * @param entity HTTP 响应实体
     * @return 如果解析成功且权限通过返回 true，否则返回 false
     */
    /*private boolean parseAuthResult(HttpEntity entity) {
        // 示例解析逻辑，可以根据实际返回结果实现
        // 假设响应体为 JSON 格式 {"allowed": true/false}
        try (InputStream is = entity.getContent()) {
            JSONObject jsonResponse = JSON.parseObject(is, StandardCharsets.UTF_8, JSONObject.class);
            return jsonResponse.getBooleanValue("allowed");
        } catch (IOException e) {
            log.warn("Failed to parse auth result", e);
            return false;
        }
    }*/

    /**
     * 创建拒绝的 {@link CheckResult} 对象。
     *
     * @param reason 拒绝的原因
     * @return {@link CheckResult} 包含拒绝信息
     */
    /*private CheckResult createRejectCheckResult(String reason) {
        return CheckResult.newBuilder()
                .setError(Error.newBuilder()
                        .setReason(reason)
                        .build())
                .build();
    }*/
}