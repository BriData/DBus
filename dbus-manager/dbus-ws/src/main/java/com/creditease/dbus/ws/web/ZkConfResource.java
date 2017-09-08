/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.ws.web;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.ws.common.Charset;
import com.creditease.dbus.ws.common.Constants;
import com.creditease.dbus.ws.common.Result;
import com.creditease.dbus.ws.domain.ZkNode;
import com.creditease.dbus.ws.service.source.SourceFetcher;
import com.creditease.dbus.ws.tools.ZookeeperServiceProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.ACL;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 数据源resource,提供数据源的相关操作
 */
@Path("/zookeeper")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")
public class ZkConfResource {
    /**
     * 根据path获取对应zk树。
     * @return zk tree json
     */
    @GET
    @Path("/loadZkTreeOfPath")
    public Response loadZkTreeOfPath(@QueryParam("path") String path) {
        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();
        ZkNode root=initialTreeOfPath(path,zkService); //根据path，生成（公共祖先的）父子关系链
        ZkNode lastCommonParent=getLastCommonParent(root);
        lastCommonParent.setChildren(loadChildrenRecursively(zkService,lastCommonParent));//获取子树
        return Response.status(200).entity(root).build();
    }

    /**
     * 根据path获取对应节点的zk子树。
     * @return zk tree json
     */
    @GET
    @Path("/loadSubTreeOfPath")
    public Response loadSubTreeOfPath(@QueryParam("path") String path) {
        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();
        ZkNode root=initialTreeOfPath(path,zkService); //根据path，生成（公共祖先的）父子关系链
        ZkNode lastCommonParent=getLastCommonParent(root);
        List<ZkNode> children=loadChildrenRecursively(zkService,lastCommonParent);//获取子树
        return Response.status(200).entity(children).build();
    }

    /**
     * 根据path获取对应节点的zk子树。
     * 每次获取三层目录
     * @return zk tree json
     */
    @GET
    @Path("/loadLevelOfPath")
    public Response loadLevelOfPath(@QueryParam("path") String path) {
        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();
        ZkNode nodeOfPath=new ZkNode();
        nodeOfPath.setPath(path);
        nodeOfPath.setName(path.substring(path.lastIndexOf("/")));
        nodeOfPath.setChildren(loadNodeRecursively(nodeOfPath, zkService, 1));
        return Response.status(200).entity(nodeOfPath).build();
    }
    private List<ZkNode> loadNodeRecursively(ZkNode nodeOfPath, IZkService zkService, int level) {
        List<ZkNode> childrenNode = new ArrayList();
        List<String> children = null;
        String path = nodeOfPath.getPath();
        try{
            children = zkService.getChildren(path);
            if(children.size() > 0){
                Collections.sort(children, (s1, s2) -> s1.compareToIgnoreCase(s2));
                /* Collections.sort(children, new Comparator() {
                    public int compare(Object o1, Object o2) {
                        String s1 = (String)o1;
                        String s2 = (String)o2;
                        return s1.compareToIgnoreCase(s2) > 0 ? 1:-1;
                    }
                });*/
                for(String child : children){
                    ZkNode node=new ZkNode(child);
                    String childPath = null;
                    if(path.equals("/")){
                        childPath = path + child;
                    }else{
                        childPath = path + "/" + child;
                    }
                    node.setPath(childPath);
                    childrenNode.add(node);
                    //System.out.println("loadding zkNode......." + node.getPath());
                    if(level > 0){
                        loadNodeRecursively(node, zkService, level-1);
                    }
                }
                nodeOfPath.setChildren(childrenNode);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return childrenNode;
    }
    /**
     * 根据给定dsName获取对应业务线的zk配置树。
     * 特别地：我们获取business tree的时候，会考虑“以zk配置模板为基准”。
     * 我们获取完整的zk配置模板树，然后根据该树的结构和映射规则，去查询业务的对应zk配置。
     * 如果没找到，会设置exists=false标志。
     * 所以，返回给前端的数据，实际上是zk模板树，但其中的path等根据规则替换成了对应business的path。
     * 并根据业务node是否存在，设置标志。
     * @return zk tree json
     */
    @GET
    @Path("/loadZkTreeByDsName/{dsName}")
    public Response loadZkTreeByDsName(@PathParam("dsName") String dsName) {
        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();
        ZkNode root = initialTreeOfPath(Constants.DBUS_ROOT,zkService);
        root.setToggled(true);
        ZkNode businessRoot=getLastCommonParent(root);
        List<ZkNode> businessConfChildren = loadBusinessConf(zkService, dsName);
        businessRoot.setChildren(businessConfChildren);
        return Response.status(200).entity(root).build();
    }

    /**
     * 根据path,添加新节点到对应zk树。
     * @return
     */
    @POST
    @Path("/addZkNodeOfPath")
    public Response addZkNodeOfPath(@QueryParam("path") String path,@QueryParam("nodeName") String nodeName) {
        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();
        String newNodePath = path.concat(nodeName);
        try {
            zkService.createNode(newNodePath, null);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(200).build();
    }
    /**
     * 根据path,删除对应zk节点。
     * @return
     */
    @GET
    @Path("/deleteZkNodeOfPath")
    public Response deleteZkNodeOfPath(@QueryParam("path") String path) {
        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();
        try {
            //zkService.deleteNode(path);
            deleteNodeRecursively(path, zkService);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(200).build();
    }
    /*
    * 递归删除给定路径的zk结点
    * */
    private void deleteNodeRecursively(String path, IZkService zkService){
        List<String> children = null;
        try{
            children = zkService.getChildren(path);
            if(children.size() > 0){
                for(String child:children){
                    String childPath = null;
                    if(path.equals("/")){
                        childPath = path + child;
                    }else{
                        childPath = path + "/" + child;
                    }
                    deleteNodeRecursively(childPath, zkService);
                }
            }
            zkService.deleteNode(path);
            System.out.println("deleting zkNode......." + path);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 根据指定search value,搜索对应zk节点。
     * @return
     */
    /*
    @GET
    @Path("/searchNodeOfValue")
    public Response searchNodeOfValue(@QueryParam("searchValue") String searchValue, @QueryParam("selectPath") String selectPath) {
        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();
        ZkNode nodeOfPath=new ZkNode();
        try {
            //nodeOfPath.setChildren(searchNodeRecursively(searchValue, selectPath, zkService));

        }catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(200).entity(nodeOfPath).build();
    }*/
    /*
    private List<ZkNode> searchNodeRecursively(String searchValue, String selectPath, IZkService zkService) {
        System.out.println(searchValue);
        System.out.println(selectPath);
        List<ZkNode> childrenNode = new ArrayList();
        List<String> children = null;
        try{
            children = zkService.getChildren(selectPath);
            if(children.size() > 0){
                for(String child : children){
                    if(child.toLowerCase().indexOf(searchValue.toLowerCase()) > -1){
                        //
                    }
                    ZkNode node=new ZkNode(child);
                    String childPath = null;
                    if(path.equals("/")){
                        childPath = path + child;
                    }else{
                        childPath = path + "/" + child;
                    }
                    node.setPath(childPath);
                    childrenNode.add(node);
                    //System.out.println("loadding zkNode......." + node.getPath());
                    if(level > 0){
                        loadNodeRecursively(node, zkService, level-1);
                    }
                }
                nodeOfPath.setChildren(childrenNode);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return new ArrayList();
    }
*/
    /**
     * 根据path获取对应节点信息。
     * @return zk node
     */
    /*
    @GET
    @Path("/getZkNodeOfPath")
    public Response getZkNodeOfPath(@QueryParam("path") String path) {
        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();
        String nodeData = "";
        Stat nodeStat = new Stat();

        try {
            nodeData = new String(zkService.getData(path),"UTF-8");
            nodeStat = zkService.getStat(path);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(200).entity(nodeData, nodeStat).build();
    }
*/

    /**
     ** clone configure from template
     */
    @GET
    @Path("/cloneConfFromTemplate")
    public Response cloneConfFromTemplate(Map<String,Object> map) {
        String TEMPLATE_ROOT_TOPOLOGY = "/DBus/ConfTemplates/Topology";
        String TEMPLATE_ROOT_EXTRACTOR = "/DBus/ConfTemplates/Extractor";

        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();

        try {
            String dsName = String.valueOf(map.get("dsName"));
            String dsType = String.valueOf(map.get("dsType"));

            cloneNodeRecursively(TEMPLATE_ROOT_TOPOLOGY, dsName, zkService);
            if(dsType.toLowerCase().equals("mysql")){
                cloneNodeRecursively(TEMPLATE_ROOT_EXTRACTOR, dsName, zkService);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return Response.status(200).build();
    }

    //  /DBus/ConfTemplates/Topology   /DBus/ConfTemplates/Extractor
    private  void cloneNodeRecursively(String templatePath, String dsName, IZkService zkService){
        // TODO
        String TEMPLATE_NODE_NAME = "/ConfTemplates";
        try{
            List<String> children = zkService.getChildren(templatePath);
            if(children.size() > 0){
                for(String nodeName:children){
                    String templateNodePath = templatePath+"/"+nodeName;
                    //判断节点是否存在，如果不存在，则创建
                    String businessNodePath = templateNodePath.replace(TEMPLATE_NODE_NAME,"");
                    businessNodePath = businessNodePath.replaceAll("placeholder",dsName);
                    if(!zkService.isExists(businessNodePath)){
                        zkService.createNode(businessNodePath, null);
                    }
                    cloneNodeRecursively(templateNodePath, dsName, zkService);
                }
            }else{
                populateLeafNodeData(templatePath, dsName, zkService);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private void populateLeafNodeData(String templatePath, String dsName,  IZkService zkService){
        // TODO
        String TEMPLATE_NODE_NAME = "/ConfTemplates";
        String businessNodePath = templatePath.replace(TEMPLATE_NODE_NAME,"");
        businessNodePath = businessNodePath.replaceAll("placeholder",dsName);  //替换占位符placeholder为dsName
        try {
            if(zkService.isExists(businessNodePath)){
                String nodeData = new String(zkService.getData(templatePath),"UTF-8");
                if(nodeData != null){
                    nodeData = nodeData.replaceAll("placeholder", dsName);
                }else{
                    nodeData = "";
                }
                zkService.setData(businessNodePath, nodeData.getBytes());
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private ZkNode initialTreeOfPath(String pathParam,IZkService zkService){
        //我们的地址是 /DBus/xxx的形式。需要对第一个 / 进行特殊处理,生成其代表的根节点
        ZkNode root=new ZkNode("/");
        root.setPath("/");
        root.setExisted(true);
        // 根据给定的路径前缀，生成父子关系链，直至给定路径对应的最后一级的节点（最后一个公共父节点）
        String[] pathArr=pathParam.split("/");
        ZkNode parent = root;
        if(pathArr.length>0){
            for(String nodeName:pathArr) {
                if(StringUtils.isNotBlank(nodeName)){
                    ZkNode node=new ZkNode(nodeName);
                    String curPath=parent.getPath()+"/"+nodeName;
                    if(parent.getPath().equals("/")){ //我们的地址是 /DBus/xxx的形式。需要对第一个 / 进行特殊处理.
                        curPath=parent.getPath()+nodeName;
                    }
                    node.setPath(curPath);
                    try {
                        boolean nodeExisted = zkService.isExists(curPath);
                        node.setExisted(zkService.isExists(curPath));
                        if(nodeExisted){
                            byte[] data = zkService.getData(curPath);
                            if (data != null && data.length > 0) {
                                node.setContent(new String(data, Charset.UTF8));
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    List<ZkNode> children=new ArrayList<>();
                    children.add(node);
                    parent.setChildren(children);
                    parent=node;
                }
            }
        }
        return root;
    }

    private List<ZkNode> loadChildrenRecursively(IZkService zkService, ZkNode parent){
        List<ZkNode> childrenNodes=new ArrayList();
        List<String> children= null;
        try {
            children = zkService.getChildren(parent.getPath());
            if(children.size()>0){
                for(String nodeName:children){
                    ZkNode node=new ZkNode(nodeName);
                    String path=parent.getPath()+"/"+nodeName;
                    if(parent.getPath().equals("/")){ //我们的地址是 /DBus/xxx的形式。需要对第一个 / 进行特殊处理.
                        path=parent.getPath()+nodeName;
                    }
                    node.setPath(path);
                    byte[] data = zkService.getData(path);
                    if (data != null && data.length > 0) {
                        node.setContent(new String(data, Charset.UTF8));
                    }
                    node.setExisted(true);
                    childrenNodes.add(node);
                }
                parent.setChildren(childrenNodes);
                for(ZkNode node:childrenNodes){
                    loadChildrenRecursively(zkService, node);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return childrenNodes;
    }
    private List<ZkNode> loadBusinessConf(IZkService zkService, String dsName){
        ZkNode root=initialTreeOfPath(Constants.TEMPLATE_ROOT,zkService);
        ZkNode templateRoot=getLastCommonParent(root);
        // 获取具体配置模板节点
        List<ZkNode> confNodes=loadChildrenRecursively(zkService,templateRoot);
        // 根据dsName把配置模板渲染成业务配置节点
        if(confNodes.size()>0) {
            for (ZkNode node : confNodes) {
                renderBusinessTreeBasedTemplate(zkService, node, dsName);
            }
        }
        return  confNodes;
    }
    private void renderBusinessTreeBasedTemplate(IZkService zkService, ZkNode zkNode, String dsName){
            zkNode.setName(zkNode.getName().replace(Constants.BUSSINESS_PLACEHOLDER,dsName));
            String path=zkNode.getPath();
            path=path.replace(Constants.TEMPLATE_NODE_NAME,"");
            path=path.replace(Constants.BUSSINESS_PLACEHOLDER,dsName);
            zkNode.setPath(path);
            try {
                // 检查是否存在与dsName对应的业务配置节点，设置标志。并且，存在的话，获取具体配置内容
                boolean exists=zkService.isExists(path);
                zkNode.setExisted(exists);
                if(exists){
                    byte[] data = zkService.getData(path);
                    if (data != null && data.length > 0) {
                        zkNode.setContent(new String(data, Charset.UTF8));
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        List<ZkNode> children=zkNode.getChildren();
        if(children!=null&&!children.isEmpty()){
            for (ZkNode node : children) {
                renderBusinessTreeBasedTemplate(zkService, node, dsName);
            }
        }
    }

    private ZkNode getLastCommonParent(ZkNode parent){
        ZkNode lastCommonParent=parent;
        lastCommonParent.setToggled(true);
        List<ZkNode> children=lastCommonParent.getChildren();
        if(children==null||children.size()==0){
            return  lastCommonParent;
        }else{
            lastCommonParent=children.get(0); // 这个方法只在“公共祖先链（每层只有一个节点）”的情况下使用，所以直接get(0)即可。
            return getLastCommonParent(lastCommonParent);
        }
    }
}
