package com.creditease.dbus.canal.auto.deploy.container;

import com.creditease.dbus.canal.auto.deploy.vo.ZkVo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.io.BufferedWriter;

/**
 * User: 王少楠
 * Date: 2018-08-06
 * Desc:
 */
public class CuratorContainer {
    private static CuratorContainer container;

    private CuratorFramework curator;


    private CuratorContainer() {
    }

    public static CuratorContainer getInstance() {
        if (container == null) {
            synchronized (CuratorContainer.class) {
                if (container == null)
                    container = new CuratorContainer();
            }
        }
        return container;
    }

    public boolean register(ZkVo conf) {
        boolean isOk = true;
        try {
            curator = CuratorFrameworkFactory.newClient(
                    conf.getZkStr(),
                    conf.getZkSessionTimeout(),
                    conf.getZkConnectionTimeout(),
                    new RetryNTimes(Integer.MAX_VALUE, conf.getZkRetryInterval()));
            curator.start();
        } catch (Exception e) {
            isOk = false;
        }
        return isOk;
    }

    public CuratorFramework getCurator() {
        return curator;
    }

    public void checkZkNodeExist(String path,BufferedWriter bw) throws Exception{
        try {
            bw.write("-----check canal zk path  begin ");
            bw.newLine();
            bw.write("node path: " + path );
            bw.newLine();
            boolean isExists = curator.checkExists().forPath(path) == null ? false : true;
            if (!isExists) {
                bw.write("path does not exist. ");
                bw.newLine();
                bw.write("creating zk path: "+path+" ...");
                bw.newLine();
                curator.create().creatingParentsIfNeeded().forPath(path, null);
                bw.write("create zk path:{ "+path+"}  done.");
                bw.newLine();
                bw.write("-----check canal zk path  end ");
                bw.newLine();
            } else {
                bw.write("path " + path +"exist");
                bw.newLine();
                bw.write("-----check canal zk path  end ");
                bw.newLine();
            }
        } catch (Exception e) {
            bw.write("-----check canal zk path  error!! ");
            bw.newLine();
            throw new RuntimeException("check znode: " + path + "error.");
        }
    }

    public void close() {
        curator.close();
    }
}
