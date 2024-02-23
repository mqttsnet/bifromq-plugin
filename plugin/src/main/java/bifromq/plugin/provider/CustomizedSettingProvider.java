package bifromq.plugin.provider;

import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;

/**
 * -----------------------------------------------------------------------------
 * File Name: CustomizedSettingProvider
 * -----------------------------------------------------------------------------
 * Description:
 * <a href="https://bifromq.io/zh-Hans/docs/plugin/setting_provider/">...</a>
 * 自定义运行时变更的设置项
 * <p>
 * 1. 实现ISettingProvider接口
 * 2. 通过@Extension注解标记为插件
 * 3. 实现provide方法，根据setting和tenantId返回对应的设置项
 * 4. 解析处理setting和tenantId，返回对应的设置项
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
@Extension
@Slf4j
public class CustomizedSettingProvider implements ISettingProvider {

    @Override
    public <R> R provide(Setting setting, String tenantId) {
        if (setting == Setting.DebugModeEnabled) {
            Boolean r = checkDebugMode(tenantId);
            log.debug("Debug mode for tenant {} is {}", tenantId, r);
            return (R) r;
        }
        return setting.current(tenantId);
    }

    private boolean checkDebugMode(String tenantId) {
        // TODO Custom logic to check debug mode

        return true;
    }

    @Override
    public void close() {
        // Custom close actions if necessary
        ISettingProvider.super.close();
    }
}
