/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
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

import com.creditease.dbus.msgencoder.ExternalEncoders;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;


/**
 * Created by zhenlinzhong on 2017/9/14.
 */
@Deprecated
public class DistributedLock implements Lock, Watcher {
    private Logger LOG = LoggerFactory.getLogger(getClass());
    private ZooKeeper zk = null;

    private String path;

    // 根节点
    private String ROOT_LOCK = "/DBus";
    private String PLUGINS_PATH = "/DBus/Commons/encoderPlugins";
    // 竞争的资源
    private String lockName;
    // 当前锁
    private String CURRENT_LOCK;

    // 等待的前一个锁
    private String WAIT_LOCK;

    // 计数器
    private CountDownLatch countDownLatch;

    private int sessionTimeout = 30000;
    private List<Exception> exceptionList = new ArrayList<Exception>();

    /**
     * 配置分布式锁
     *
     * @param zkConnect 连接的url
     * @param lockName  竞争资源
     *                  分布式锁实现原理：
     *                  1.客户端调用create()方法创建名为“_locknode_/guid-lock-”的节点，节点的创建类型需要设置为EPHEMERAL_SEQUENTIAL。
     *                  2.客户端调用getChildren(“_locknode_”)方法来获取所有已经创建的子节点，同时在这个节点上注册子节点变更通知的Watcher。
     *                  3.客户端获取到所有子节点path之后，如果发现自己在步骤1中创建的节点是所有节点中序号最小的，那么就认为这个客户端获得了锁。
     *                  4.如果在步骤3中发现自己并非是所有子节点中最小的，说明自己还没有获取到锁，就开始等待，直到下次子节点变更通知的时候，
     *                  再进行子节点的获取，判断是否获取锁。
     */
    public DistributedLock(String path, String zkConnect, String lockName) {
        this.lockName = lockName;

        this.path = path;

        try {
            //连接zookeeper
            zk = new ZooKeeper(zkConnect, sessionTimeout, this);

            Stat stat = zk.exists(ROOT_LOCK, false);
            if (stat == null) {
                // 如果根节点不存在，则创建根节点
                zk.create(ROOT_LOCK, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            Stat stat2 = zk.exists(PLUGINS_PATH, false);
            if (stat2 == null) {
                // 如果节点不存在，则创建节点
                zk.create(PLUGINS_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }


    /**
     * 节点监视器
     */
    public void process(WatchedEvent event) {
        if (this.countDownLatch != null) {
            this.countDownLatch.countDown();
        }
    }


    public boolean tryLock() {
        try {
            String splitStr = "_lock_";
            if (lockName.contains(splitStr)) {
                throw new LockException("锁名有误");
            }
            //创建临时有序节点
            CURRENT_LOCK = zk.create(ROOT_LOCK + "/" + lockName + splitStr, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            LOG.info(Thread.currentThread().getName() + ": " + CURRENT_LOCK + "已经创建!");
            //取所有子节点
            List<String> subNodes = zk.getChildren(ROOT_LOCK, false);
            //取出所有lockName的锁
            List<String> lockObjects = new ArrayList<>();
            for (String node : subNodes) {
                String _node = node.split(splitStr)[0];
                if (_node.equals(lockName)) {
                    lockObjects.add(node);
                }
            }
            Collections.sort(lockObjects);
            LOG.info(Thread.currentThread().getName() + " 的锁是 " + CURRENT_LOCK);
            //若当前节点为最小节点，则获取锁成功
            if (CURRENT_LOCK.equals(ROOT_LOCK + "/" + lockObjects.get(0))) {
                return true;
            }

            // 若不是最小节点，则找到自己的前一个节点
            String prevNode = CURRENT_LOCK.substring(CURRENT_LOCK.lastIndexOf("/") + 1);
            WAIT_LOCK = lockObjects.get(Collections.binarySearch(lockObjects, prevNode) - 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        try {
            if (this.tryLock()) {
                return true;
            }
            return waitForLock(WAIT_LOCK, timeout);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    public void lock() {
        if (exceptionList.size() > 0) {
            throw new LockException(exceptionList.get(0));
        }
        try {
            if (this.tryLock()) {
                LOG.info(Thread.currentThread().getName() + " " + lockName + "获得了锁！");
                LOG.info(Thread.currentThread().getName() + " " + lockName + "正在运行！");
                LOG.info(Thread.currentThread().getName() + " " + lockName + "写zookeeper!");
                //将扫描到的脱敏类型写入zookeeper
                ExternalEncoders.saveEncoderType(path);
                //LOG.info(Thread.currentThread().getName() + " " + lockName + "成功释放了锁！");
            } else {
                // 等待锁
                //LOG.info(Thread.currentThread().getName() + " " + lockName + "等待获取锁...");
                waitForLock(WAIT_LOCK, sessionTimeout);
                //读zk
                LOG.info(Thread.currentThread().getName() + " " + lockName + "读zookeeper!");
                ExternalEncoders.getEncoderTypeFromZk(path);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    // 等待锁
    private boolean waitForLock(String prev, long waitTime) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(ROOT_LOCK + "/" + prev, true);

        if (stat != null) {
            LOG.info(Thread.currentThread().getName() + "等待锁 " + ROOT_LOCK + "/" + prev);
            this.countDownLatch = new CountDownLatch(1);
            // 计数等待，若等到前一个节点消失，则precess中进行countDown，停止等待，获取锁
            this.countDownLatch.await(waitTime, TimeUnit.MILLISECONDS);
            this.countDownLatch = null;
            LOG.info(Thread.currentThread().getName() + " 等到了锁!");
        }
        return true;
    }

    //关闭zk等操作
    public void unlock() {
        try {
            LOG.info(Thread.currentThread().getName() + "释放锁 " + CURRENT_LOCK);
            zk.delete(CURRENT_LOCK, -1);
            CURRENT_LOCK = null;
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public Condition newCondition() {
        return null;
    }


    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }


    public class LockException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public LockException(String e) {
            super(e);
        }

        public LockException(Exception e) {
            super(e);
        }
    }


    public static void distributedLock(String path, String zkConnect) {
        DistributedLock dLock = null;
        try {
            dLock = new DistributedLock(path, zkConnect, "test");
            dLock.lock();
        } finally {
            dLock.unlock();
        }
    }


}


