package com.bytegen.common.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc:
 */
public class ZKFacade {
    private static Logger logger = LoggerFactory.getLogger(ZKFacade.class);

    private static final ZKEnvironmentSetting currentEnvironmentSetting = new ZKEnvironmentSetting();

    /**
     * Only environment default setting will be cached in the map
     */
    private static final Map<String, ZKEnvironmentSetting> environmentSettingMap = new HashMap<>();

    public static ZKClient getClient() {
        ZKEnvironmentSetting setting = environmentSettingMap
                .computeIfAbsent(currentEnvironmentSetting.getZKEnvironment(), env -> currentEnvironmentSetting);
        return new ZKClient(setting.getZKEnvironment(), setting.getZKServer(), setting.getZKAuth());
    }

    public static ZKClient getClient(String environment) {
        ZKEnvironmentSetting setting = environmentSettingMap
                .computeIfAbsent(environment, env -> new ZKEnvironmentSetting(environment));
        return new ZKClient(setting.getZKEnvironment(), setting.getZKServer(), setting.getZKAuth());
    }

    public static ZKClient getClient(String server, String auth) {
        return new ZKClient("", server, auth);
    }
}
