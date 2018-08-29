package com.bytegen.common.zookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc: Use holder to reuse the underlying ZkClient connection, so make it static.
 */
class ZKClientHolder {
    private static final Logger logger = LoggerFactory.getLogger(ZKClientHolder.class);

    private static final ConcurrentMap<String, Pair<CuratorFramework, AtomicInteger>> serverAuthClientMap = new ConcurrentHashMap<>();

    private static final int SESSION_TIMEOUT = 30000;
    private static final int CONNECTION_TIMEOUT = 30000;
    private static final RetryPolicy DEFAULT_RETRY_POLICY = new ExponentialBackoffRetry(1000, 5);

    private static String serverAuthKey(String server, String auth) {
        Validate.notBlank(server, "Server is blank");
        if (StringUtils.isBlank(auth)) {
            return server;
        }
        return server + "-->" + auth;
    }

    static synchronized void tryCreateClient(String server, String auth) {
        String key = serverAuthKey(server, auth);
        if (serverAuthClientMap.get(key) == null) {
            Validate.notBlank(server, "Zk server is blank");

            CuratorFramework client = newClient(server, auth, SESSION_TIMEOUT, CONNECTION_TIMEOUT);
            serverAuthClientMap.put(key, Pair.of(client, new AtomicInteger(0)));
        }
        serverAuthClientMap.get(key).getValue().incrementAndGet();
        logger.info("Server [{}] auth [****] client count [{}] after create",
                server, serverAuthClientMap.get(key).getValue().get());
    }

    static synchronized void tryCloseClient(String server, String auth) {
        String key = serverAuthKey(server, auth);
        Pair<CuratorFramework, AtomicInteger> clientPair = serverAuthClientMap.get(key);
        if (clientPair != null) {
            logger.info("Server [{}] auth [****] client count [{}] before close",
                    server, serverAuthClientMap.get(key).getValue().get());
            if (clientPair.getValue().decrementAndGet() == 0) {
                clientPair.getKey().close();
                serverAuthClientMap.remove(key);
            }
        }
    }

    static CuratorFramework getClient(String server, String auth) {
        String key = serverAuthKey(server, auth);
        return serverAuthClientMap.get(key).getKey();
    }

    private static CuratorFramework newClient(String server, String auth, int sessionTimeout, int connectionTimeout) {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(server)
                .sessionTimeoutMs(sessionTimeout)
                .connectionTimeoutMs(connectionTimeout)
                .retryPolicy(DEFAULT_RETRY_POLICY);
        if (null != auth) {
            builder.authorization("digest", auth.getBytes());
        }

        CuratorFramework client = builder.build();
        client.getConnectionStateListenable().addListener((client1, newState) -> {
            switch (newState) {
                case CONNECTED:
                    logger.info("connected to zookeeper: " + server);
                    break;
                case SUSPENDED:
                    logger.warn("suspended to zookeeper: " + server);
                    break;
                case RECONNECTED:
                    logger.info("reconnected to zookeeper: " + server);
                    break;
                case LOST:
                    logger.error("lose connection to zookeeper: " + server);
                    break;
                case READ_ONLY:
                    logger.info("read only model to zookeeper: " + server);
                    break;
            }
        });
        client.start();
        return client;
    }

    private ZKClientHolder() {
        // Nothing
    }


}
