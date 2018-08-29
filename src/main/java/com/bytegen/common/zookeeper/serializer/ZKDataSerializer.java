package com.bytegen.common.zookeeper.serializer;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc: Serialization of zookeeper data
 */
public interface ZKDataSerializer<T> {

    T deserialize(byte[] bytes);

    byte[] serialize(T data);
}
