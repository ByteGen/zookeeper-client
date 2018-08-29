package com.bytegen.common.zookeeper.serializer;

import com.bytegen.common.zookeeper.ZKConstant;

import java.io.*;
import java.util.Properties;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc:
 */
public class PropertiesSerializer implements ZKDataSerializer<Properties> {

    private PropertiesSerializer() {
    }

    private static final PropertiesSerializer instance = new PropertiesSerializer();

    public static PropertiesSerializer getInstance() {
        return instance;
    }


    @Override
    public Properties deserialize(byte[] bytes) {
        try (Reader inputReader = new InputStreamReader(new ByteArrayInputStream(bytes), ZKConstant.DEFAULT_CHARSET)) {
            Properties p = new Properties();
            p.load(inputReader);
            return p;
        } catch (IOException e) {
            throw new RuntimeException("Deserialize properties failed.", e);
        }
    }

    @Override
    public byte[] serialize(Properties data) {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream();
             Writer propertiesWriter = new OutputStreamWriter(os, ZKConstant.DEFAULT_CHARSET)) {

            data.store(propertiesWriter, "Serialized by ZKClient -- PropertiesSerializer");
            return os.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Serialize properties failed.", e);
        }
    }
}
