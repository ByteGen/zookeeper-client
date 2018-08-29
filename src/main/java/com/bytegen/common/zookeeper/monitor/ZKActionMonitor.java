package com.bytegen.common.zookeeper.monitor;

import com.bytegen.common.zookeeper.ZKActionType;
import com.bytegen.common.zookeeper.ZKClient;
import com.bytegen.common.zookeeper.ZKConstant;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc: Monitor zk actions.
 */
public class ZKActionMonitor {
    private static final Logger logger = LoggerFactory.getLogger(ZKActionMonitor.class);

    /**
     * All action listeners, would receive the zk action events.
     */
    private final List<ZKActionListener> subscribers;

    /**
     * Execute on action events
     */
    private final ExecutorService monitorExecutor;

    private ZKActionMonitor() {
        subscribers = new ArrayList<>();
        subscribers.add(new LoggerActionListener());

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("zookeeper-monitor-subscribe-thread-%d")
                .setUncaughtExceptionHandler(
                        (t, e) -> logger.error(String.format("Zookeeper action monitor, thread[%s] throw : ", t.getName()), e))
                .build();
        monitorExecutor = Executors.newSingleThreadExecutor(namedThreadFactory);
    }

    public List<ZKActionListener> getSubscribers() {
        return subscribers;
    }

    class LoggerActionListener implements ZKActionListener {

        @Override
        public void onAction(ZKActionType type, ZKClient client, String path, Object data) {
            logger.info("ZooKeeper event: type [{}], server [{}], path [{}], data [{}]",
                    type.name(), client.getServer(), path, stringData(data));
        }

        private String stringData(Object data) {
            if (null == data) {
                return null;
            }

            if (data instanceof String) {
                return (String) data;
            }

            if (data instanceof byte[]) {
                return new String((byte[]) data, ZKConstant.DEFAULT_CHARSET);
            }

            return String.valueOf(data);
        }
    }

    // singleton
    private static ZKActionMonitor instance = new ZKActionMonitor();

    public static ZKActionMonitor getInstance() {
        return instance;
    }


    ///////////////////////
    // subscribe methods //

    public boolean addSubscriber(ZKActionListener listener) {
        if (listener == null) {
            return false;
        }
        return subscribers.add(listener);
    }

    public boolean removeSubscriber(ZKActionListener listener) {
        if (listener == null) {
            return false;
        }
        return subscribers.remove(listener);
    }

    public void triggerAction(ZKActionType type, ZKClient client,
                              String path, Object data) {
        monitorExecutor.execute(() -> {
            for (ZKActionListener listener : subscribers) {
                listener.onAction(type, client, path, data);
            }
        });
    }
}
