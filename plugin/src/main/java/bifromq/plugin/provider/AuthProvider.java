package bifromq.plugin.provider;

import bifromq.plugin.config.ConfigUtil;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.*;
import com.baidu.bifromq.type.ClientInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.pf4j.Extension;
import org.springframework.http.HttpStatus;

import java.util.Optional;
import java.util.concurrent.*;

/**
 * -----------------------------------------------------------------------------
 * File Name: EventKafkaProvider
 * -----------------------------------------------------------------------------
 * Description:
 * <a href="https://bifromq.io/zh-Hans/docs/plugin/auth_provider/">...</a>
 * Auth Provider插件旨在为BifroMQ运行时提供验证MQTT客户端连接和授权发布/订阅消息主题的能力
 *
 * 1. 实现IAuthProvider接口
 * 2. 通过@Extension注解标记为插件
 * 3. 实现auth方法，调用ThingLinks 的认证接口验证客户端连接
 * 4. 实现check方法，验证客户端是否有权限执行指定的操作
 *
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

@Extension
@Slf4j
public final class AuthProvider implements IAuthProvider {

    /**
     * You should fill in the thinglinks platform authentication interface that you want to call
     */
    private final String clientConnectionUrl;

    private final ThreadPoolExecutor executor;

    public AuthProvider() {
        // Create a thread pool based on the actual requirements
        int corePoolSize = Runtime.getRuntime().availableProcessors() * 10;
        int maximumPoolSize = corePoolSize * 20;
        long keepAliveTime = 60L;
        TimeUnit unit = TimeUnit.SECONDS;
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();

        this.executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime,
                unit, workQueue, threadFactory, handler);
        this.clientConnectionUrl = ConfigUtil.getPluginConfig().getAuthProviderConfig().getAuthConnectionUrl();
    }

    @Override
    public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
        String clientId = authData.getClientId();
        String password = authData.getPassword().toStringUtf8();
        String username = authData.getUsername();
        log.info("Authenticating client - clientId: {}, username: {}, password: {}", clientId, username, password);

        if (StringUtils.isNotBlank(clientId) && StringUtils.isNotBlank(password) && StringUtils.isNotBlank(username)) {
            return CompletableFuture.supplyAsync(() -> clientConnectionAuthentication(clientId, password, username), executor)
                    .thenApply(this::handleAuthenticationResponse);
        } else {
            return CompletableFuture.completedFuture(createRejectResponse());
        }
    }

    private HttpResponse clientConnectionAuthentication(String clientId, String password, String username) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("clientIdentifier", clientId);
        jsonObject.put("password", password);
        jsonObject.put("username", username);
        jsonObject.put("protocolType", "MQTT");
        jsonObject.put("authMode", 0);

        return HttpUtil.createPost(clientConnectionUrl)
                .body(jsonObject.toJSONString())
                .execute();
    }

    private MQTT3AuthResult handleAuthenticationResponse(HttpResponse response) {
        int statusCode = response.getStatus();
        String responseBody = response.body();
        log.info("Authentication response - statusCode: {}, responseBody: {}", statusCode, responseBody);

        if (statusCode == HttpStatus.OK.value()) {
            return parseAuthResponse(responseBody);
        } else {
            return createRejectResponse();
        }
    }

    private MQTT3AuthResult parseAuthResponse(String responseBody) {
        JSONObject responseJson = JSON.parseObject(responseBody);
        boolean certificationResult = responseJson.getBoolean("certificationResult");

        if (certificationResult) {
            Optional<JSONObject> deviceResultJson = Optional.ofNullable(responseJson.getJSONObject("deviceResult"));
            String clientId = deviceResultJson.flatMap(json -> Optional.ofNullable(json.getString("clientId")))
                    .orElse("");
            String tenantId = Optional.ofNullable(responseJson.getString("tenantId"))
                    .orElse("");

            log.info("Authentication successful - clientId: {}, tenantId: {}", clientId, tenantId);

            return MQTT3AuthResult.newBuilder()
                    .setOk(Ok.newBuilder()
                            .setTenantId(tenantId)
                            .setUserId(clientId)
                            .build())
                    .build();
        } else {
            log.info("Authentication failed");
            return createRejectResponse();
        }
    }

    private MQTT3AuthResult createRejectResponse() {
        log.info("Authentication rejected");
        return MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                        .setCode(Reject.Code.NotAuthorized)
                        .build())
                .build();
    }

    @Override
    public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
        return CompletableFuture.completedFuture(true);
    }
}
