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

import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.utils.ZkConfTemplateHelper;
import com.creditease.dbus.ws.common.Charset;
import com.creditease.dbus.ws.common.Constants;
import com.creditease.dbus.ws.domain.ZkNode;
import com.creditease.dbus.ws.tools.ZookeeperServiceProvider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.*;

import static com.creditease.dbus.commons.Constants.DBUS_CONF_TEMPLATE_ROOT;
import static com.creditease.dbus.commons.Constants.TEMPLATE_NODE_NAME;

/**
 * 数据源resource,提供数据源的相关操作
 */
@Path("/zookeeper")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")
public class ZkConfResource {
    private Logger logger = LoggerFactory.getLogger(getClass());
    /**
     * 根据path获取对应zk树。
     * @return zk tree json
     */
    @GET
    @Path("/loadZkTreeOfPath")
    public Response loadZkTreeOfPath(@QueryParam("path") String path) {
        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();
        ZkNode root=initialTreeOfPath(path, false, zkService); //根据path，生成（公共祖先的）父子关系链
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
        ZkNode root=initialTreeOfPath(path, false, zkService); //根据path，生成（公共祖先的）父子关系链
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
    @Path("/loadZkTreeByDsName/{dsName}/{dsType}")
    public Response loadZkTreeByDsName(@PathParam("dsName") String dsName, @PathParam("dsType") String dsType) {
        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();
        ZkNode root = initialTreeOfPath(com.creditease.dbus.commons.Constants.DBUS_ROOT, false, zkService);
        root.setToggled(true);
        ZkNode businessRoot=getLastCommonParent(root);
        List<ZkNode> businessConfChildren = loadBusinessConf(zkService, dsName, dsType);
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
    public Response deleteZkNodeOfPath(Map<String, Object> map) {
        String path = map.get("path").toString();
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
        IZkService zkService = ZookeeperServiceProvider.getInstance().getZkService();
        try {
            String dsName = String.valueOf(map.get("dsName"));
            String dsType = String.valueOf(map.get("dsType"));
            // 根据数据源类型，获取对应数据源需要克隆的zk子树。依次克隆
            Set<String> rootNodesSetOfDs = ZkConfTemplateHelper.getZkConfRootNodesSetForDs(DbusDatasourceType.parse(dsType));
            if(null!=rootNodesSetOfDs && !rootNodesSetOfDs.isEmpty()){
                for (String rootNodePath:rootNodesSetOfDs) {
                    // 公共节点可能被重复处理，例如：/DBus/ConfTemplaes/Topology，可能被处理多次。
                    // 但结果幂等。为了代码简洁性，选择接受重复处理，而不是各种判断，特殊处理，增加代码复杂性,导致代码不可读。
                    initialTreeOfPath(com.creditease.dbus.commons.Constants.DBUS_CONF_TEMPLATE_ROOT+"/"+rootNodePath, true, zkService);
                    cloneNodeRecursively(com.creditease.dbus.commons.Constants.DBUS_CONF_TEMPLATE_ROOT+"/"+rootNodePath, dsName, dsType, zkService);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return Response.status(200).build();
    }

    private  void cloneNodeRecursively(String templatePath, String dsName, String dsType, IZkService zkService){
        try{
            List<String> children = zkService.getChildren(templatePath);
            if(children.size() > 0){
                for(String nodeName:children){
                    String templateNodePath = templatePath+"/"+nodeName;
                    //判断节点是否存在，如果不存在，则创建
                    String businessNodePath = templateNodePath.replace(TEMPLATE_NODE_NAME,"");
                    businessNodePath = businessNodePath.replaceAll("typePlaceholder", dsType);
                    businessNodePath = businessNodePath.replaceAll("placeholder",dsName);
                    if(!zkService.isExists(businessNodePath)){
                        // 公共节点可能被重复处理，例如：/DBus/ConfTemplaes/Topology，可能被处理多次。
                        // 但结果幂等。为了代码简洁性，选择接受重复处理，而不是各种判断，特殊处理，增加代码复杂性,导致代码不可读。
                        initialTreeOfPath(businessNodePath, true, zkService);
                    }
                    cloneNodeRecursively(templateNodePath, dsName, dsType, zkService);
                }
            }else{
                populateLeafNodeData(templatePath, dsName, dsType,  zkService);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void populateLeafNodeData(String templatePath, String dsName, String dsType, IZkService zkService){
        String businessNodePath = templatePath.replace(TEMPLATE_NODE_NAME,"");
        businessNodePath = businessNodePath.replaceAll("typePlaceholder", dsType);
        businessNodePath = businessNodePath.replaceAll("placeholder",dsName);  //替换占位符placeholder为dsName
        try {
            if(zkService.isExists(businessNodePath)){
                String nodeData = new String(zkService.getData(templatePath),"UTF-8");
                if(nodeData != null){
                    nodeData = nodeData.replaceAll("typePlaceholder", dsType);
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

    private ZkNode initialTreeOfPath(String pathParam, boolean createIfNotExists, IZkService zkService){
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
                        if(!nodeExisted&&createIfNotExists){
                            try {
                                zkService.createNode(curPath, null);
                                nodeExisted = true;
                            } catch (Exception e) {
                                logger.warn("Create zk Node failed.",e);
                            }
                        }

                        node.setExisted(zkService.isExists(curPath));
                        if(nodeExisted){
                            byte[] data = zkService.getData(curPath);
                            if (data != null && data.length > 0) {
                                node.setContent(new String(data, Charset.UTF8));
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Initial Tree Of Path failed.",e);
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

    private List<ZkNode> loadBusinessConf(IZkService zkService, String dsName, String dsType) {
        ZkNode root = initialTreeOfPath(DBUS_CONF_TEMPLATE_ROOT, false, zkService);
        ZkNode templateRoot = getLastCommonParent(root);

        if (StringUtils.isNotBlank(dsType)) {
            Set<String> rootNodesSetOfDs = ZkConfTemplateHelper.getZkConfRootNodesSetForDs(DbusDatasourceType.parse(dsType));
            // 根据dsType, 确定需要加载的各子树的root。
            for (String rootNodePath : rootNodesSetOfDs) {
                ZkNode parentNode = templateRoot;
                String parentPath = parentNode.getPath();
                String[] pathItems = rootNodePath.split("/");
                for (String nodeName : pathItems) {
                    String currentNodePath = parentPath + "/" + nodeName;
                    ZkNode zkNode = new ZkNode();
                    zkNode.setName(nodeName);
                    zkNode.setToggled(true); // TODO
                    zkNode.setPath(currentNodePath);
                    if (!parentNode.hasChild(nodeName)) {
                        if(parentNode.getChildren() == null)
                            parentNode.setChildren(new ArrayList<>());
                        parentNode.getChildren().add(zkNode);
                    }
                    parentPath = currentNodePath;
                    parentNode = parentNode.getChildByName(nodeName);
                }
                //  最末级节点，也就是退出for循环时，parentNode代表的节点，是我们根据dsType筛出来的，要加载的子树的root。根据这个root，加载子树
                parentNode.setChildren(loadChildrenRecursively(zkService, parentNode));
            }
        }

        // 上述步骤结束后，生成了根据不同dsType，需要加载的conf template子孙节点级。进一步处理templateRoot的子（孙—）节点。
        List<ZkNode> confTemplateChildren = templateRoot.getChildren();
        // 根据dsName把配置模板渲染成业务配置节点
        if (confTemplateChildren!=null && !confTemplateChildren.isEmpty()) {
            for (ZkNode node : confTemplateChildren) {
                renderBusinessTreeBasedTemplate(zkService, node, dsName);
            }
        }
        return confTemplateChildren;
    }

    private void renderBusinessTreeBasedTemplate(IZkService zkService, ZkNode zkNode, String dsName) {
        zkNode.setName(zkNode.getName().replace(Constants.BUSSINESS_PLACEHOLDER, dsName));
        String path = zkNode.getPath();
        path = path.replace(TEMPLATE_NODE_NAME, "");
        path = path.replace(Constants.BUSSINESS_PLACEHOLDER, dsName);
        zkNode.setPath(path);
        try {
            // 检查是否存在与dsName对应的业务配置节点，设置标志。并且，存在的话，获取具体配置内容
            boolean exists = zkService.isExists(path);
            zkNode.setExisted(exists);
            if (exists) {
                byte[] data = zkService.getData(path);
                if (data != null && data.length > 0) {
                    zkNode.setContent(new String(data, Charset.UTF8));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<ZkNode> children = zkNode.getChildren();
        if (children != null && !children.isEmpty()) {
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
