package com.bytegen.common.zookeeper;

import com.bytegen.common.zookeeper.serializer.StringSerializer;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;

public class ZKClientTest {

    private String zkHost = "10.125.253.8:2181,10.125.252.116:2181,10.125.252.65:2181";
    private String zkAuth = "";

    private ZKClient client;

    @Before
    public void init() {
        client = ZKFacade.getClient(zkHost, zkAuth);
    }

    @After
    public void close() throws Exception {
        client.close();
    }

    @Test
    public void exists() throws Exception {
        boolean root = client.exists("/");
        Assert.assertTrue(root);
    }

    @Test
    public void getZKStat() throws Exception {
        Stat zkStat = client.getZKStat("/");
        Assert.assertTrue(null != zkStat);

        Stat zkStat1 = client.getZKStat("/abcdefg");
        Assert.assertTrue(null == zkStat1);
    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void getData() throws Exception {
        client.getData("/abcdefg");
    }

    @Test
    public void getData1() throws Exception {
        Stat stat = new Stat();
        byte[] data = client.getData("com/bytegen/dev", stat);

        Assert.assertTrue(null != data);
        Assert.assertTrue(stat.getCtime() > 0);
    }

    @Test
    public void getData2() throws Exception {
        String data = client.getData("com/bytegen/dev", StringSerializer.getInstance());
        Assert.assertTrue(null != data && data.length() > 0);
    }

    @Test
    public void getData3() throws Exception {
        Stat stat = new Stat();
        String data = client.getData("com/bytegen/dev", stat, StringSerializer.getInstance());
        Assert.assertTrue(null != data && data.length() > 0);
        Assert.assertTrue(stat.getCtime() > 0);
    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void deletePath() throws Exception {
        client.deletePath("abcdefg", false);
    }

    @Test(expected = KeeperException.NotEmptyException.class)
    public void createEphemeral() throws Exception {
        client.createEphemeral("abcd/efg");
        client.deletePath("abcd", false);
    }

    @Test
    public void createEphemeral1() throws Exception {
        client.createEphemeral("abcd/efg", "test", StringSerializer.getInstance());
        String data = client.getData("abcd/efg", StringSerializer.getInstance());
        Assert.assertTrue("test".equals(data));

        client.deletePath("abcd", true);
    }

    @Test
    public void createEphemeralSequential() throws Exception {
        String p1 = client.createEphemeralSequential("abcd/efg");
        String p2 = client.createEphemeralSequential("abcd/efg");

        Assert.assertTrue(p2.compareTo(p1) > 0);
    }

    @Test
    public void createEphemeralSequential1() throws Exception {
        String p1 = client.createEphemeralSequential("abcd/efg", "test", StringSerializer.getInstance());

        String data = client.getData(p1, StringSerializer.getInstance());
        Assert.assertTrue("test".equals(data));
    }

    @Test
    public void createPersistent() throws Exception {
    }

    @Test
    public void createPersistent1() throws Exception {
        // client.createPersistent("com/bytegen/dev/10005", "{}", new StringSerializer());
    }

    @Test
    public void createPersistentOrSetData() throws Exception {
    }

    @Test
    public void setData() throws Exception {
    }

    @Test
    public void setData1() throws Exception {
    }

    @Test
    public void getChildrenNames() throws Exception {
        List<String> names = client.getChildrenNames("/com");
        Assert.assertTrue(null != names && names.size() > 0);
    }

    @Test
    public void getNodeCache() throws Exception {
        NodeCache cache = client.getNodeCache("com/bytegen/dev");
        cache.getListenable().addListener(() -> {
            // void...
        });
    }

    @Test
    public void getPathChildCache() throws Exception {
        PathChildrenCache cache = client.getPathChildCache("com/bytegen/dev");
        cache.getListenable().addListener((client, event) -> {
            // void...
        });
    }

    @Test
    public void getTreeCache() throws Exception {
        TreeCache cache = client.getTreeCache("com/bytegen/dev");
        cache.getListenable().addListener((client, event) -> {
            // void...
        });
    }

    @Test
    public void addNodeCacheListener() throws Exception {
        String path = "com/bytegen/dev";
        NodeCacheListener listener = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                ChildData data = client.getNodeCache(path).getCurrentData();
                if (null != data && null != data.getData()) {
                    // do something
                }
            }
        };
        client.addNodeCacheListener(path, listener);
        Assert.assertThat(client.getNodeCache(path).getListenable().size(), is(1));
    }

    @Test
    public void removeNodeCacheListener() throws Exception {
        String path = "com/bytegen/dev";
        NodeCacheListener listener = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                ChildData data = client.getNodeCache(path).getCurrentData();
                if (null != data && null != data.getData()) {
                    // do something
                }
            }
        };
        client.addNodeCacheListener(path, listener);
        Assert.assertThat(client.getNodeCache(path).getListenable().size(), is(1));
        client.removeNodeCacheListener(path, listener);
        Assert.assertThat(client.getNodeCache(path).getListenable().size(), is(0));
    }

}