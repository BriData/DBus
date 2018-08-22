package com.creditease.dbus.canal.auto.deploy.utils;

import com.creditease.dbus.canal.auto.deploy.bean.DeployPropsBean;
import com.creditease.dbus.canal.auto.deploy.container.CuratorContainer;
import com.creditease.dbus.canal.auto.deploy.vo.ZkVo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * User: 王少楠
 * Date: 2018-08-09
 * Desc:
 */
public class ZKUtils {
    public static ZkVo checkStr(String userStr){
        ZkVo zkVo = ZkVo.loadZk();
        //防止用户输错，替换中文符号
        userStr.replaceAll("，",",");
        userStr.replaceAll("：",":");
        String[] zkStrs = userStr.split(",");
        for(String zkStr: zkStrs){
            String[] info = zkStr.split(":");
            if(info.length != 2){
                continue;
            }
            if(checkConn(info[0],Integer.valueOf(info[1]))){
                zkVo.setZkStr(zkStr);
                break;
            }
        }
        return zkVo;
    }

    private static boolean checkConn(String address,int port){
        try {
            bindPort(address, port);
            return true;
        }catch (IOException e){
            return false;
        }

    }

    private static void bindPort(String host,int port) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress(host,port));
        s.close();
    }

    public static void checkZKNode(DeployPropsBean deployProps, BufferedWriter bw)throws Exception{
        try {
            bw.write("------------ check canal zk node begin ------------ ");
            bw.newLine();
            bw.write("zk str:  "+deployProps.getZkPath());
            bw.newLine();
            ZkVo zkVo = checkStr(deployProps.getZkPath());
            if(zkVo.getZkStr() == null){
                bw.write("zk connect fail... ");
                bw.newLine();
                throw new Exception();
            }
            bw.write("zk path :  "+zkVo.getZkStr());
            bw.newLine();
            boolean connectResult = CuratorContainer.getInstance().register(zkVo);
            if (!connectResult) {
                throw new Exception();

            } else {
                String canalPath = "/DBus"+"/"+"Canal"+"/canal-"+deployProps.getDsName();
                CuratorContainer.getInstance().checkZkNodeExist(canalPath,bw);
            }
            bw.write("------------ check canal zk node end ------------ ");
            bw.newLine();
        }catch (Exception e){
            bw.write("------------ check canal zk node fail ------------ ");
            bw.newLine();
            throw e;
        }
    }

}
