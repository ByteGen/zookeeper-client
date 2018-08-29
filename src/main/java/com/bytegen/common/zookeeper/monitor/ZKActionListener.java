package com.bytegen.common.zookeeper.monitor;

import com.bytegen.common.zookeeper.ZKActionType;
import com.bytegen.common.zookeeper.ZKClient;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc:
 */
public interface ZKActionListener {
    void onAction(ZKActionType type, ZKClient client, String path, Object data);
}
