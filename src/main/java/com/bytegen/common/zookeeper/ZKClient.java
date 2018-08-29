package com.bytegen.common.zookeeper;

import com.bytegen.common.zookeeper.monitor.ZKActionMonitor;
import com.bytegen.common.zookeeper.serializer.ZKDataSerializer;
import org.apache.commons.lang3.Validate;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc: zookeeper client
 */
public final class ZKClient {

    private final String environment;
    private final String server;
    private final String auth;

    private final Map<String, NodeCache> nodeCacheMap = new HashMap<>();
    private final Map<String, PathChildrenCache> pathChildrenCacheMap = new HashMap<>();
    private final Map<String, TreeCache> treeCacheMap = new HashMap<>();

    ZKClient(String environment, String server, String auth) {
        ZKClientHolder.tryCreateClient(server, auth);
        this.environment = environment;
        this.server = server;
        this.auth = auth;
    }

    public String getEnvironment() {
        return environment;
    }

    public String getServer() {
        return server;
    }

    public Map<String, NodeCache> getNodeCacheMap() {
        return nodeCacheMap;
    }

    public Map<String, PathChildrenCache> getPathChildrenCacheMap() {
        return pathChildrenCacheMap;
    }

    public Map<String, TreeCache> getTreeCacheMap() {
        return treeCacheMap;
    }

    public void close() {
        ZKClientHolder.tryCloseClient(server, auth);
    }

    public CuratorFramework getFramework() {
        return ZKClientHolder.getClient(server, auth);
    }


    //////////////////////////////
    // zookeeper client methods //

    /**
     * Gets the real Zookeeper node path.
     */
    public String getRealPath(String path) {
        return ZKPaths.makePath(path, null);
    }

    /**
     * Test whether the node specified by path exists or not.
     */
    public boolean exists(final String path) throws Exception {
        Stat zkStat = getZKStat(getRealPath(path));
        return zkStat != null;
    }

    /**
     * Get Stat of the node specified by path.
     */
    public Stat getZKStat(final String path) throws Exception {
        return getFramework().checkExists().forPath(getRealPath(path));
    }

    /**
     * Get the data of given path.
     */
    public byte[] getData(final String path) throws Exception {
        byte[] bytes = getFramework().getData().forPath(getRealPath(path));
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.GET_DATA, this, path, bytes);
        return bytes;
    }

    public <T> T getData(final String path, final ZKDataSerializer<T> serializer) throws Exception {
        String realPath = getRealPath(path);
        byte[] bytes = getFramework().getData().forPath(realPath);
        T data = null;
        if (null != bytes) {
            data = serializer.deserialize(bytes);
        }
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.GET_DATA, this, realPath, data);
        return data;
    }

    /**
     * Get data and Stat of given path.
     */
    public byte[] getData(final String path, final Stat stat) throws Exception {
        Validate.notNull(stat, "Stat can not be null");

        String realPath = getRealPath(path);
        byte[] bytes = getFramework().getData().storingStatIn(stat).forPath(realPath);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.GET_DATA, this, realPath, bytes);
        return bytes;
    }

    public <T> T getData(final String path, final Stat stat, final ZKDataSerializer<T> serializer) throws Exception {
        Validate.notNull(stat, "Stat can not be null");

        String realPath = getRealPath(path);
        byte[] bytes = getFramework().getData().storingStatIn(stat).forPath(realPath);
        T data = null;
        if (null != bytes) {
            data = serializer.deserialize(bytes);
        }
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.GET_DATA, this, realPath, data);
        return data;
    }

    /**
     * Delete the given path.
     */
    public void deletePath(final String path, final boolean deleteChildren) throws Exception {
        String realPath = getRealPath(path);
        if (deleteChildren) {
            getFramework().delete().deletingChildrenIfNeeded().forPath(realPath);
        } else {
            getFramework().delete().forPath(realPath);
        }
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.DELETE_PATH, this, realPath, null);
    }


    /**
     * Create a ephemeral node.
     */
    public void createEphemeral(final String path) throws Exception {
        String realPath = getRealPath(path);
        getFramework().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(realPath);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.CREATE_EPHEMERAL, this, realPath, null);
    }

    public <T> void createEphemeral(final String path, final T data, final ZKDataSerializer<T> serializer) throws Exception {
        Validate.notNull(data, "Data can't be null.");

        String realPath = getRealPath(path);
        byte[] nodeData = serializer.serialize(data);
        getFramework().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(realPath, nodeData);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.CREATE_EPHEMERAL, this, realPath, nodeData);
    }

    /**
     * Create a ephemeral and sequential node.
     */
    public String createEphemeralSequential(final String path) throws Exception {
        String result = getFramework().create().creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(getRealPath(path));
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.CREATE_EPHEMERAL, this, result, null);
        return result;
    }

    public <T> String createEphemeralSequential(final String path, final T data, final ZKDataSerializer<T> serializer) throws Exception {
        Validate.notNull(data, "Data can't be null.");

        byte[] nodeData = serializer.serialize(data);
        String result = getFramework().create().creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(getRealPath(path), nodeData);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.CREATE_EPHEMERAL, this, result, nodeData);
        return result;
    }

    /**
     * Create persistent node.
     */
    public void createPersistent(final String path) throws Exception {
        String realPath = getRealPath(path);
        getFramework().create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT).forPath(realPath);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.CREATE_PERSISTENT, this, realPath, null);
    }

    public <T> void createPersistent(final String path, final T data, final ZKDataSerializer<T> serializer) throws Exception {
        Validate.notNull(data, "Data can't be null.");

        String realPath = getRealPath(path);
        byte[] nodeData = serializer.serialize(data);
        getFramework().create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT).forPath(realPath, nodeData);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.CREATE_PERSISTENT, this, realPath, nodeData);
    }

    /**
     * Create persistent node or update data if exist.
     */
    public <T> void createPersistentOrSetData(final String path, final T data, final ZKDataSerializer<T> serializer) throws Exception {
        Validate.notNull(data, "Data can't be null.");

        String realPath = getRealPath(path);
        byte[] nodeData = serializer.serialize(data);
        getFramework().create().orSetData().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT).forPath(realPath, nodeData);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.UPDATE_PERSISTENT, this, realPath, nodeData);
    }

    /**
     * Set node data.
     */
    public <T> void setData(final String path, final T data, final ZKDataSerializer<T> serializer) throws Exception {
        Validate.notNull(data, "Data can't be null.");

        String realPath = getRealPath(path);
        byte[] nodeData = serializer.serialize(data);
        getFramework().setData().forPath(realPath, nodeData);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.SET_DATA, this, realPath, nodeData);
    }

    public <T> void setData(final String path, final T data, final ZKDataSerializer<T> serializer, final int expectedVersion) throws Exception {
        Validate.notNull(data, "Data can't be null.");

        String realPath = getRealPath(path);
        byte[] nodeData = serializer.serialize(data);
        getFramework().setData().withVersion(expectedVersion).forPath(realPath, nodeData);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.SET_DATA, this, realPath, nodeData);
    }

    /**
     * Get all children names of given parent path.
     */
    public List<String> getChildrenNames(final String path) throws Exception {
        String realPath = getRealPath(path);
        List<String> names = getFramework().getChildren().forPath(realPath);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.GET_CHILD_NAMES, this, realPath, names);
        return names;
    }

    /**
     * Add new node cache
     */
    public NodeCache getNodeCache(final String path) throws Exception {
        String realPath = getRealPath(path);
        synchronized (nodeCacheMap) {
            NodeCache cache = nodeCacheMap.get(realPath);
            if (null == cache) {
                cache = new NodeCache(getFramework(), realPath);
                cache.start();
                nodeCacheMap.put(realPath, cache);
                ZKActionMonitor.getInstance().triggerAction(ZKActionType.ADD_NODE_CACHE, this, realPath, null);
            }
            return cache;
        }
    }

    public NodeCache addNodeCacheListener(final String path, final NodeCacheListener nodeCacheListener) throws Exception {
        Validate.notBlank(path, "Node cache path can't be blank.");
        Validate.notNull(nodeCacheListener, "Node cache listener can't be null.");

        String realPath = getRealPath(path);
        NodeCache cache = getNodeCache(realPath);
        cache.getListenable().addListener(nodeCacheListener);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.ADD_CACHE_LISTENER, this, realPath, null);
        return cache;
    }

    public NodeCache removeNodeCacheListener(final String path, final NodeCacheListener nodeCacheListener) throws Exception {
        Validate.notBlank(path, "Node cache path can't be blank.");
        Validate.notNull(nodeCacheListener, "Node cache listener can't be null.");

        String realPath = getRealPath(path);
        NodeCache cache = getNodeCache(realPath);
        cache.getListenable().removeListener(nodeCacheListener);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.REMOVE_CACHE_LISTENER, this, realPath, null);
        return cache;
    }

    /**
     * Add new children cache
     */
    public PathChildrenCache getPathChildCache(final String path) throws Exception {
        String realPath = getRealPath(path);
        synchronized (pathChildrenCacheMap) {
            PathChildrenCache cache = pathChildrenCacheMap.get(realPath);
            if (null == cache) {
                cache = new PathChildrenCache(getFramework(), realPath, true);
                cache.start();
                pathChildrenCacheMap.put(realPath, cache);
                ZKActionMonitor.getInstance().triggerAction(ZKActionType.ADD_PATH_CACHE, this, realPath, null);
            }
            return cache;
        }
    }

    public PathChildrenCache addPathChildCacheListener(final String path, final PathChildrenCacheListener listener) throws Exception {
        Validate.notBlank(path, "Path child cache path can't be blank.");
        Validate.notNull(listener, "Path children cache listener can't be null.");

        String realPath = getRealPath(path);
        PathChildrenCache cache = getPathChildCache(realPath);
        cache.getListenable().addListener(listener);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.ADD_CACHE_LISTENER, this, realPath, null);
        return cache;
    }

    public PathChildrenCache removePathChildCacheListener(final String path, final PathChildrenCacheListener listener) throws Exception {
        Validate.notBlank(path, "Path child cache path can't be blank.");
        Validate.notNull(listener, "Path children cache listener cache listener can't be null.");

        String realPath = getRealPath(path);
        PathChildrenCache cache = getPathChildCache(realPath);
        cache.getListenable().removeListener(listener);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.REMOVE_CACHE_LISTENER, this, realPath, null);
        return cache;
    }

    /**
     * Add new tree cache
     */
    public TreeCache getTreeCache(final String path) throws Exception {
        String realPath = getRealPath(path);
        synchronized (treeCacheMap) {
            TreeCache cache = treeCacheMap.get(realPath);
            if (null == cache) {
                cache = new TreeCache(getFramework(), realPath);
                cache.start();
                treeCacheMap.put(realPath, cache);
                ZKActionMonitor.getInstance().triggerAction(ZKActionType.ADD_TREE_CACHE, this, realPath, null);
            }
            return cache;
        }
    }

    public TreeCache addTreeCacheListener(final String path, final TreeCacheListener listener) throws Exception {
        Validate.notBlank(path, "Tree cache path can't be blank.");
        Validate.notNull(listener, "Tree cache listener can't be null.");

        String realPath = getRealPath(path);
        TreeCache cache = getTreeCache(realPath);
        cache.getListenable().addListener(listener);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.ADD_CACHE_LISTENER, this, realPath, null);
        return cache;
    }

    public TreeCache removeTreeCacheListener(final String path, final TreeCacheListener listener) throws Exception {
        Validate.notBlank(path, "Tree cache path can't be blank.");
        Validate.notNull(listener, "Tree cache listener cache listener can't be null.");

        String realPath = getRealPath(path);
        TreeCache cache = getTreeCache(realPath);
        cache.getListenable().removeListener(listener);
        ZKActionMonitor.getInstance().triggerAction(ZKActionType.REMOVE_CACHE_LISTENER, this, realPath, null);
        return cache;
    }
}
