/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */

package com.creditease.dbus.commons;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkService implements IZkService {
    // 0.4.0 秘钥
    private static final String auth = "DBus:CHEOi@TSeyLfSact";
    // 0.3.0 秘钥
    // private static final String auth = "DBus:yvr5skHKWJOR0jiG";
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CuratorFramework client;

    private HashMap<String, String> tableVersions;
    private HashMap<String, Long> cache;



    /**
     * 创建ZK连接
     * @param connectString  ZK服务器地址列表
     */
    public ZkService(String connectString) throws Exception {
        this(connectString, 30000);
    }

    /**
     * 创建ZK连接
     * @param connectString  ZK服务器地址列表
     * @param sessionTimeout   Session超时时间
     */
    public ZkService(String connectString, int sessionTimeout) throws Exception {
        CuratorFrameworkFactory.Builder builder;
        builder = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .namespace("")
                .authorization("digest", auth.getBytes())
                .retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000))
                .connectionTimeoutMs(sessionTimeout);

        client = builder.build();
        client.start();
        if(!client.blockUntilConnected(20, TimeUnit.SECONDS)) {
            throw new Exception("zookeeper connected failed!");
        }

        tableVersions = new HashMap<>();
        cache = new HashMap<>();
    }

    /**
     * 获得节点状态值
     * @param path
     * @return
     * @throws Exception
     */
    @Override
    public Stat getStat(String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        return stat;
    }

    /**
     * 获得节点ACL信息
     * @param path
     * @return
     * @throws Exception
     */
    @Override
    public Map<String, Object> getACL(String path) throws Exception {
        ACL acl = client.getACL().forPath(path).get(0);
        Id id = acl.getId();
        HashMap<String, Object> map = new HashMap<>();
        map.put("perms",acl.getPerms());
        map.put("id",id.getId());
        map.put("scheme",id.getScheme());
        return map;
    }

    /**
     * 获得节点的version号，如果节点不存在，返回 -1
     * @param path
     * @return
     * @throws Exception
     */
    @Override
    public int getVersion(String path) throws Exception {
        Stat stat = this.getStat(path);
        if (stat != null) {
            return stat.getVersion();
        } else {
            return -1;
        }
    }

    /**
     * 节点是否存在
     * @param path 节点path
     */
    @Override
    public boolean isExists(String path) throws Exception {
        Stat stat = this.getStat(path);
        if (stat != null) {
            return true;
        } else {
            return false;
        }
    }


    /**
     *  创建节点
     * @param path 节点path
     * @param payload 初始数据内容
     * @return
     */
    @Override
    public void createNode(String path, byte[] payload) throws Exception {
        client.create().creatingParentsIfNeeded().forPath(path, payload);
        logger.info("节点创建成功, Path: " + path );
    }


    /**
     * createNodeWithACL
     * Create a node under ACL mode
     * @param path
     * @param payload
     * @throws Exception
     */
    public void createNodeWithACL(String path, byte[] payload) throws Exception {
        ACL acl = new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS);
        List<ACL> aclList = Lists.newArrayList(acl);
        try {
            client.create().withACL(aclList).forPath(path, payload);
        } catch (Exception e) {
            logger.error("Create security file failed.");
            e.printStackTrace();
        }
    }

    /**
     * 删除指定节点
     * @param path 节点path
     */
    @Override
    public void deleteNode(String path) throws Exception {
        client.delete().forPath(path);
        logger.info("节点删除成功, Path: " + path );
    }

    /**
     * 删除指定节点
     * @param path 节点path
     */
    @Override
    public void rmr(String path) throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath(path);
        logger.info("节点删除成功, Path: " + path );
    }

    /**
     * 读取指定节点数据内容
     * @param path 节点path
     * @return
     */
    @Override
    public byte[] getData(String path) throws Exception {
        byte[] data =  client.getData().forPath(path);
        //logger.info("获取数据成功，path：" + path );
        return data;
    }


    /**
     * 更新指定节点数据内容
     * @param path 节点path
     * @param payload  数据内容
     * @return
     */
    @Override
    public boolean setData(String path, byte[] payload) throws Exception {
        Stat stat = client.setData().forPath(path, payload);
        if (stat != null) {
            //logger.info("设置数据成功，path：" + path );
            return true;
        } else {
            logger.error("设置数据失败，path：" + path );
            return false;
        }
    }

    /**
     * CAS更新指定节点数据内容
     * @param path  节点path
     * @param payload  数据内容
     * @param version 版本号
     * @return
     * @throws Exception
     */
    @Override
    public int setDataWithVersion(String path, byte[] payload, int version) throws Exception {
        try {
            Stat stat = null;
            if (version != -1) {
                stat = client.setData().withVersion(version).forPath(path, payload);
            } else {
                stat = client.setData().forPath(path, payload);
            }
            if (stat != null) {
                //logger.info("CAS设置数据成功，path：" + path );
                return stat.getVersion();
            } else {
                logger.error("CAS设置数据失败，path : {}", path);
                return -1;
            }
        } catch (KeeperException.BadVersionException ex) {
            logger.error("CAS设置数据失败，path : {},error msg : {}", path, ex.getMessage());
            return -1;
        }

    }

    /**
     * 获得properties
     * @param path  节点path
     */
    @Override
    public Properties getProperties(String path) throws Exception {
        byte[] data = getData(path);
        Properties props = new Properties();
        props.load(new ByteArrayInputStream(data));
        return props;
    }

    /**
     * 设置properties
     * @param path  节点path
     * @param props properties
     */
    @Override
    public boolean setProperties(String path, Properties props) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        props.store(os, null);
        byte[] data = os.toByteArray();

        return setData(path, data);
    }


    /**
     * 获得下一个值
     * @param dbName
     * @param schemaName
     * @param tableName
     * @param version
     * @return
     * @throws Exception
     */
    @Override
    public long nextValue(String dbName, String schemaName, String tableName, String version) throws Exception  {
        String nameSpace = String.format("%s.%s.%s.%s", dbName, schemaName, tableName, version);
        return nextValue(nameSpace);
    }

    /**
     * 获得下一个值
     * @param nameSpace
     * @return
     * @throws Exception
     */
    @Override
    public long nextValue(String nameSpace) throws Exception  {
        nameSpace = nameSpace.toUpperCase();

        String [] arr = nameSpace.split("\\.");
        if (arr.length != 4) {
            throw new IllegalArgumentException("格式错误！正确格式为: db.schema.table.version ");
        }

        Long val = cache.get(nameSpace);
        if (val == null) {
            //从zk上取，或者创建新节点
            String path = Constants.NAMESPACE_ROOT + "/" + nameSpace.replace('.', '/');
            val = nextValueFromZk(path);
        } else {
            long nextVal = val + 1;
            if (nextVal % SEQUENCE_STEP == SEQUENCE_START) {
                //如果达到上限，需要从zk再去新的一批值
                String path = Constants.NAMESPACE_ROOT + "/" + nameSpace.replace('.', '/');
                val = nextValueFromZk(path);
            } else {
                val = nextVal;
            }
        }
        updateCache(nameSpace, val);
        return val;
    }

    /**
     * 只保存一个version的数据在map中和zk中
     * @param nameSpace
     */
    private void updateCache(String nameSpace, long value) throws Exception{
        String[] arr = nameSpace.split("\\.");
        String table = String.format("%s.%s.%s", arr[0], arr[1], arr[2]);
        String version = arr[3];
        String oldVersion = tableVersions.get(table);
        if(oldVersion == null) {
            tableVersions.put(table, version);
        }
        else {
            if ( !oldVersion.equals(version)) {
                String oldNameSpace = table + "." + oldVersion;
                cache.remove(oldNameSpace);

                //从zk上上删除旧节点
                String path = Constants.NAMESPACE_ROOT + "/" + oldNameSpace.replace('.', '/');
                try {
                    deleteNode(path);
                }catch (Exception e) {
                    logger.warn("Failed to delete zk node:"+path+"! Please confirm it exists.");
                }
                tableVersions.put(table, version);
            }
        }
        cache.put(nameSpace, value);
    }


    /**
     * 使用DistributedAtomicLong 在zk上创建节点或读取节点数据，数据为long值。
     * @param path
     * @return
     * @throws Exception
     */
    private long nextValueFromZk(String path) throws Exception {
        if (isExists(path)) {
            DistributedAtomicLong count = new DistributedAtomicLong(client, path, new RetryNTimes(10, 1000));
            AtomicValue<Long> val = count.add(SEQUENCE_STEP);
            return val.preValue();
        } else {
            DistributedAtomicLong count = new DistributedAtomicLong(client, path, new RetryNTimes(10, 1000));
            count.forceSet(SEQUENCE_START);
            AtomicValue<Long> val = count.add(SEQUENCE_STEP);
            //byte[] data = getData(path);
            return val.preValue();
        }
    }

    /**
     * 获得当前值
     * @param dbName
     * @param schemaName
     * @param tableName
     * @param version
     * @return
     * @throws Exception
     */
    @Override
    public long currentValue(String dbName, String schemaName, String tableName, String version) throws Exception  {
        String nameSpace = String.format("%s.%s.%s.%s", dbName, schemaName, tableName, version);
        return currentValue(nameSpace);
    }

    /**
     * 获得当前值
     * @param nameSpace
     * @return
     * @throws Exception
     */
    @Override
    public long currentValue(String nameSpace) throws Exception {
        nameSpace = nameSpace.toUpperCase();
        String [] arr = nameSpace.split("\\.");
        if (arr.length != 4) {
            throw new IllegalArgumentException("格式错误！正确格式为: db.schema.table.version ");
        }

        Long val = cache.get(nameSpace);
        if (val == null) {
            //从zk中获得最新节点信息
            String path = Constants.NAMESPACE_ROOT + "/" + nameSpace.replace('.', '/');
            val = currentValueFromZk(path);
            cache.put(nameSpace, val);
        }
        return val;
    }


    /**
     * 获得ZK中的当前值,如果不存在，抛出异常
     * @param path
     * @return
     * @throws Exception
     */
    private long currentValueFromZk (String path) throws Exception {
        if (isExists(path)) {
            DistributedAtomicLong count = new DistributedAtomicLong(client, path, new RetryNTimes(10, 1000));
            AtomicValue<Long> val = count.get();
            return val.postValue();
        } else {
            throw new RuntimeException("Path is not existed! Call nextValue firstly!");
        }
    }

    /**
     * 获得当前值
     * @param path
     * @return
     * @throws Exception
     */
    @Override
    public List<String> getChildren(String path) throws Exception {
        return client.getChildren().forPath(path);
    }

    /**
     * 获得分布式自增变量
     * @param path
     * @return
     * @throws Exception
     */
    public Long getIncrementValue(String path) throws Exception {
        DistributedAtomicLong atomicId = new DistributedAtomicLong(client, path, new RetryNTimes(32,1000));
        AtomicValue<Long> rc = atomicId.get();
        if (rc.succeeded()) {
            logger.debug("getIncrementValue({}) success! get: {}.", path, rc.postValue());
        } else {
            logger.warn("getIncrementValue({}) failed! get: {}.", path, rc.postValue());
        }
        return rc.postValue();
    }

    /**
     * 自增并获得，自增后的变量
     * @param path
     * @return
     * @throws Exception
     */
    public Long incrementAndGetValue(String path) throws Exception {
        DistributedAtomicLong atomicId = new DistributedAtomicLong(client, path, new RetryNTimes(32,1000));
        AtomicValue<Long> rc = atomicId.increment();
        if (rc.succeeded()) {
            logger.info("incrementAndGetValue({}) success! before: {}, after: {}.", path, rc.preValue(), rc.postValue());
        } else {
            logger.warn("incrementAndGetValue({}) failed! before: {}, after: {}.", path, rc.preValue(), rc.postValue());
        }
        return rc.postValue();
    }

    @Override
    public byte[] registerWatcher(String path, Watcher watcher) throws Exception {
        return client.getData().usingWatcher(watcher).forPath(path);
    }

    /**
     * 关闭ZK连接
     */
    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    public static void main(String args[]) throws Exception {
        String newNode = "/test";
        ZkService zkService = new ZkService("localhost:2181");

        System.out.println(zkService.incrementAndGetValue("/DBus/NameSpace/aaa/bbb/ccc").toString());
        System.out.println(zkService.incrementAndGetValue("/DBus/NameSpace/aaa/bbb/ccc").toString());
        System.out.println(zkService.getIncrementValue("/DBus/NameSpace/aaa/bbb/ccc").toString());

        if (!zkService.isExists(newNode)) {
            zkService.createNode(newNode, new Date().toString().getBytes());
        }


        int version = zkService.getVersion(newNode);
        System.out.printf("get %s node version %d\n", newNode, version);

        System.out.printf("setDataWithVersion(%s, %d) node first time, result = %d\n",
                newNode, version, zkService.setDataWithVersion(newNode, new Date().toString().getBytes(), version));
        System.out.printf("setDataWithVersion(%s, %d) node second time, result = %d\n",
                newNode, version, zkService.setDataWithVersion(newNode, new Date().toString().getBytes(), version));


        List<String> zNodes = zkService.getChildren("/");
        for (String zNode : zNodes) {
            System.out.println("ChildrenNode " + zNode);
        }
        byte[] data = zkService.getData(newNode);
        System.out.println("GetData before setting:");
        System.out.println (new String(data,"UTF-8"));

        zkService.setData(newNode, "Modified data".getBytes());


        data = zkService.getData(newNode);
        System.out.println("GetData after setting:");
        System.out.println (new String(data,"UTF-8"));


        Properties props = PropertiesUtils.getProps("consumer.properties");
        zkService.setProperties(newNode, props);

        Properties props2 = zkService.getProperties(newNode);
        for (Object obj : props2.keySet() )
        {
            String valueFor=(String)props2.get(obj);

            System.out.println(obj + "=" + valueFor);
        }

        zkService.deleteNode(newNode);

        for (int i = 0; i < 1200; i++) {
            long val1 = zkService.nextValue("db1.schema1.table1.v1");
            long val2 = zkService.currentValue("db1.schema1.table1.v1");
            System.out.printf("nextValue=%d, currentValue=%d\n", val1, val2);
        }
        zkService.nextValue("db1.schema1.table1.v2");
        long val3 = zkService.currentValue("db1.schema1.table1.v1");
        System.out.printf("val3=%d\n", val3);



        zkService.close();
    }
}
