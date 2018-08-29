# zookeeper client
Simple zookeeper client tool. 

## Usage
1. 添加 pom 依赖
```
<dependency>
    <groupId>com.bytegen.common</groupId>
    <artifactId>zookeeper-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```
2. 获取 ZKClient

通过 ZKFacade.getClient 获取 ZKClient 实例, 通过实例调用包装的 zookeeper 方法.
如需使用 ZKClient 未提供的方法, 可使用 ZKClient 的 getFramework() 直接操作 CuratorFramework.
```java
public class SampleBean {
    private ZKClient getZKClient(String server, String auth) {
        ZKClient client;
        if (StringUtils.isNotBlank(server)) {
            client = ZKFacade.getClient(server, auth);
        } else {
            if (StringUtils.isNotBlank(auth)) {
                logger.warn("ZooKeeper auth unused...");
            }
            // Get zookeeper client by system property zookeeper.env
            client = ZKFacade.getClient();
        }
        return client;
    }
}
```
3. 自定义monitor

实现 ZKActionListener 接口; 并在 ZKActionMonitor 进行注册. 参考如下示例:
```java
public class LoggerActionListener implements ZKActionListener {
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
    
    static {
        ZKActionMonitor.getInstance().addSubscriber(new LoggerActionListener());
    }
}
```
4. 自定义serializer

实现 ZKDataSerializer 接口即可.