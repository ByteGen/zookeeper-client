package com.bytegen.common.zookeeper.serializer;

import com.bytegen.common.zookeeper.ZKConstant;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc:
 */
public class StringSerializer implements ZKDataSerializer<String> {

    private StringSerializer() {
    }

    private static final StringSerializer instance = new StringSerializer();
    public static StringSerializer getInstance() {
        return instance;
    }


    @Override
    public String deserialize(byte[] bytes) {
        return new String(bytes, ZKConstant.DEFAULT_CHARSET);
    }

    @Override
    public byte[] serialize(String data) {
        return data.getBytes(ZKConstant.DEFAULT_CHARSET);
    }
}
