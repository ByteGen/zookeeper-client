package com.bytegen.common.zookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc: Get zookeeper config via environment config
 */
class ZKEnvironmentSetting {
    private static final Logger logger = LoggerFactory.getLogger(ZKEnvironmentSetting.class);

    private static final String DEFAULT_ZOOKEEPER_SERVER_FILE = "zookeeper_servers.properties";

    private String environment;
    private String server;
    private String auth;

    public ZKEnvironmentSetting() {
    }

    public ZKEnvironmentSetting(String environment) {
        this.environment = environment;
    }

    public String getZKEnvironment() {
        String environment = getEnvironmentValue();
        Validate.notEmpty(environment, "ZK environment not found.");

        return environment;
    }

    public String getZKServer() {
        String zkServer = getZKServer(getZKEnvironment());
        Validate.notEmpty(zkServer, "ZK servers not found for " + getZKEnvironment());

        return zkServer;
    }

    public String getZKAuth() {
        return getZKAuth(getZKEnvironment());
    }

    /**
     * 1. Get the environment currently used from {@link System} property "zookeeper.env"
     * 2. Get property "zookeeper.env" from file "/zookeeper.properties"
     */
    private synchronized String getEnvironmentValue() {
        if (this.environment == null) {
            String zkHostSystemProperties = System.getProperty("zookeeper.env");
            if (StringUtils.isNotBlank(zkHostSystemProperties)) {
                this.environment = zkHostSystemProperties.toUpperCase();
            } else {
                Properties zk = loadProperties("zookeeper.properties");
                if (null != zk && StringUtils.isNotBlank(zk.getProperty("zookeeper.env"))) {
                    this.environment = zk.getProperty("zookeeper.env").toUpperCase();
                }
            }
        }
        return environment;
    }

    /**
     * 1. Get the servers from {@link System} property "${environment}.zookeeper.server"
     * 2. Get property "${environment}.zookeeper.server" from file "/zookeeper.properties"
     * 3. Get from DEFAULT_ZOOKEEPER_SERVER_FILE
     */
    private synchronized String getZKServer(String environment) {
        Validate.notEmpty(environment, "ZK environment not found.");

        if (this.server == null) {
            String envServerKey = environment + ".zookeeper.server";

            String zkServer = System.getProperty(envServerKey);
            if (StringUtils.isNotBlank(zkServer)) {
                this.server = zkServer;
            } else {
                Properties zk = loadProperties("zookeeper.properties");
                if (null != zk && StringUtils.isNotBlank(zk.getProperty(envServerKey))) {
                    this.server = zk.getProperty(envServerKey);
                } else {
                    Properties defaultZk = loadProperties(DEFAULT_ZOOKEEPER_SERVER_FILE);
                    if (null != defaultZk && StringUtils.isNotBlank(defaultZk.getProperty(envServerKey))) {
                        this.server = defaultZk.getProperty(envServerKey);
                    }
                }
            }
        }
        return this.server;
    }

    /**
     * 1. Get the servers from {@link System} property "${environment}.zookeeper.auth"
     * 2. Get property "${environment}.zookeeper.auth" from file "/zookeeper.properties"
     */
    private synchronized String getZKAuth(String environment) {
        Validate.notEmpty(environment, "ZK environment not found.");

        if (this.auth == null) {
            String envAuthKey = environment + ".zookeeper.auth";

            String zkServer = System.getProperty(envAuthKey);
            if (StringUtils.isNotBlank(zkServer)) {
                this.auth = zkServer;
            } else {
                Properties zk = loadProperties("zookeeper.properties");
                if (null != zk && StringUtils.isNotBlank(zk.getProperty(envAuthKey))) {
                    this.auth = zk.getProperty(envAuthKey);
                }
            }
        }
        return this.auth;
    }

    private static Properties loadProperties(String name) {
        Validate.notNull(name, "Resource cannot be null.");
        try (InputStream stream = ZKEnvironmentSetting.class.getResourceAsStream(name.startsWith("/") ? name : "/" + name)) {
            if (null != stream) {
                Properties properties = new Properties();
                properties.load(stream);
                return properties;
            }
        } catch (IOException e) {
            logger.error("load properties from resources failed: " + name, e);
        }
        return null;
    }
}
