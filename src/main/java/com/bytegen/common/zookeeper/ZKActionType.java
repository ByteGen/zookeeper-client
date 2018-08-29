package com.bytegen.common.zookeeper;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc:
 */
public enum ZKActionType {

    GET_DATA,
    DELETE_PATH,
    GET_CHILD_NAMES,
    CREATE_EPHEMERAL,
    CREATE_PERSISTENT,
    UPDATE_PERSISTENT,
    SET_DATA,
    ADD_NODE_CACHE,
    ADD_PATH_CACHE,
    ADD_TREE_CACHE,
    ADD_CACHE_LISTENER,
    REMOVE_CACHE_LISTENER,

}
