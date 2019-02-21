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

package com.creditease.dbus.service;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.domain.model.ZkNode;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.utils.ZkConfTemplateHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

import static com.creditease.dbus.commons.Constants.DBUS_CONF_TEMPLATE_ROOT;
import static com.creditease.dbus.commons.Constants.TEMPLATE_NODE_NAME;

/**
 * Created by xiancangao on 2018/05/07.
 */
@Service
public class ZkConfService {

	private Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private IZkService zkService;

	/**
	 * 根据path获取对应zk树。
	 *
	 * @return zk tree json
	 */
	public ZkNode loadZkTreeOfPath(String path) throws Exception {
		ZkNode root = initialTreeOfPath(path, false);
		ZkNode lastCommonParent = getLastCommonParent(root);
		lastCommonParent.setChildren(loadChildrenRecursively(lastCommonParent));
		logger.info("loadZkTreeOfPath request process ok. path:{}", path);
		return root;
	}

	/**
	 * 根据path获取对应节点的zk子树。
	 *
	 * @return zk tree json
	 */
	public List<ZkNode> loadSubTreeOfPath(String path) throws Exception {
		ZkNode root = initialTreeOfPath(path, false);
		ZkNode lastCommonParent = getLastCommonParent(root);
		List<ZkNode> children = loadChildrenRecursively(lastCommonParent);
		logger.info("loadSubTreeOfPath request process ok. path:{}", path);
		return children;
	}

	/**
	 * 根据path获取对应节点的zk子树。
	 * 每次获取三层目录
	 *
	 * @return zk tree json
	 */
	public ZkNode loadLevelOfPath(String path) throws Exception {
		if(!zkService.isExists(path)){
			return null;
		}
		ZkNode nodeOfPath = new ZkNode();
		nodeOfPath.setPath(path);
		nodeOfPath.setName(path.substring(path.lastIndexOf("/")));
		nodeOfPath.setChildren(loadNodeRecursively(nodeOfPath, 1));
		nodeOfPath.setStat(zkService.getStat(path));
		nodeOfPath.setAcl(zkService.getACL(path));
		logger.info("loadLevelOfPath request process ok. path:{}", path);
		return nodeOfPath;
	}

	/**
	 * 根据给定dsName获取对应业务线的zk配置树。
	 * 特别地：我们获取business tree的时候，会考虑“以zk配置模板为基准”。
	 * 我们获取完整的zk配置模板树，然后根据该树的结构和映射规则，去查询业务的对应zk配置。
	 * 如果没找到，会设置exists=false标志。
	 * 所以，返回给前端的数据，实际上是zk模板树，但其中的path等根据规则替换成了对应business的path。
	 * 并根据业务node是否存在，设置标志。
	 *
	 * @return zk tree json
	 */
	public ZkNode loadZkTreeByDsName(String dsName, String dsType) throws Exception {
		ZkNode root = initialTreeOfPath(Constants.DBUS_ROOT, false);
		root.setToggled(true);
		ZkNode businessRoot = getLastCommonParent(root);
		List<ZkNode> businessConfChildren = loadBusinessConf(dsName, dsType);
		businessRoot.setChildren(businessConfChildren);
		logger.info("loadZkTreeByDsName request process ok. dsName:{} , dsType:{}", dsName, dsType);
		return root;
	}

	/**
	 * 根据path,添加新节点到对应zk树。
	 *
	 * @return
	 */
	public void addZkNodeOfPath(String path, String nodeName) throws Exception {
		String newNodePath = path.concat(nodeName);
		zkService.createNode(newNodePath, null);
		logger.info("addZkNodeOfPath request process ok. path:{} , nodeName:{}", path, nodeName);
	}

	/**
	 * 根据path,删除对应zk节点。
	 *
	 * @return
	 */
	public String deleteZkNodeOfPath(String path) throws Exception {
		String s = deleteNodeRecursively(path);
		if (s != null) {
			return s;
		}
		logger.info("deleteZkNodeOfPath request process ok. path:{} ", path);
		return null;
	}

	/**
	 * * clone configure from template
	 */
	public void cloneConfFromTemplate(String dsName, String dsType) throws Exception {
		// 根据数据源类型，获取对应数据源需要克隆的zk子树。依次克隆
		Set<String> rootNodesSetOfDs = ZkConfTemplateHelper.getZkConfRootNodesSetForDs(DbusDatasourceType.parse(dsType));
		if (null != rootNodesSetOfDs && !rootNodesSetOfDs.isEmpty()) {
			for (String rootNodePath : rootNodesSetOfDs) {
				// 公共节点可能被重复处理，例如：/DBus/ConfTemplaes/Topology，可能被处理多次。
				// 但结果幂等。为了代码简洁性，选择接受重复处理，而不是各种判断，特殊处理，增加代码复杂性,导致代码不可读。
				initialTreeOfPath(Constants.DBUS_CONF_TEMPLATE_ROOT + "/" + rootNodePath, true);
				cloneNodeRecursively(Constants.DBUS_CONF_TEMPLATE_ROOT + "/" + rootNodePath, dsName, dsType);
			}
		}
		logger.info("cloneConfFromTemplate request process ok. dsName:{},dsType:{} ", dsName, dsType);
	}

	/**
	 * 根据path，生成（公共祖先的）父子关系链
	 *
	 * @param pathParam
	 * @param createIfNotExists
	 * @return
	 */
	private ZkNode initialTreeOfPath(String pathParam, boolean createIfNotExists) throws Exception {
		//我们的地址是 /DBus/xxx的形式。需要对第一个 / 进行特殊处理,生成其代表的根节点
		ZkNode root = new ZkNode("/");
		root.setPath("/");
		root.setExisted(true);
		// 根据给定的路径前缀，生成父子关系链，直至给定路径对应的最后一级的节点（最后一个公共父节点）
		String[] pathArr = pathParam.split("/");
		ZkNode parent = root;
		if (pathArr.length > 0) {
			for (String nodeName : pathArr) {
				if (StringUtils.isNotBlank(nodeName)) {
					ZkNode node = new ZkNode(nodeName);
					String curPath = parent.getPath() + "/" + nodeName;
					if (parent.getPath().equals("/")) { //我们的地址是 /DBus/xxx的形式。需要对第一个 / 进行特殊处理.
						curPath = parent.getPath() + nodeName;
					}
					node.setPath(curPath);
					boolean nodeExisted = zkService.isExists(curPath);
					if (!nodeExisted && createIfNotExists) {
						zkService.createNode(curPath, null);
						nodeExisted = true;

					}
					node.setExisted(zkService.isExists(curPath));
					if (nodeExisted) {
						byte[] data = zkService.getData(curPath);
						if (data != null && data.length > 0) {
							node.setContent(new String(data, "utf-8"));
						}
					}
					List<ZkNode> children = new ArrayList<>();
					children.add(node);
					parent.setChildren(children);
					parent = node;
				}
			}
		}
		return root;
	}

	/**
	 * @param parent
	 * @return
	 */
	private ZkNode getLastCommonParent(ZkNode parent) {
		ZkNode lastCommonParent = parent;
		lastCommonParent.setToggled(true);
		List<ZkNode> children = lastCommonParent.getChildren();
		if (children == null || children.size() == 0) {
			return lastCommonParent;
		} else {
			// 这个方法只在“公共祖先链（每层只有一个节点）”的情况下使用，所以直接get(0)即可。
			lastCommonParent = children.get(0);
			return getLastCommonParent(lastCommonParent);
		}
	}

	/**
	 * 获取父节点下所有子树
	 *
	 * @param parent
	 * @return
	 */
	private List<ZkNode> loadChildrenRecursively(ZkNode parent) {
		List<ZkNode> childrenNodes = new ArrayList();
		List<String> children = null;
		try {
			children = zkService.getChildren(parent.getPath());
			if (children.size() > 0) {
				for (String nodeName : children) {
					ZkNode node = new ZkNode(nodeName);
					String path = parent.getPath() + "/" + nodeName;
					if (parent.getPath().equals("/")) { //我们的地址是 /DBus/xxx的形式。需要对第一个 / 进行特殊处理.
						path = parent.getPath() + nodeName;
					}
					node.setPath(path);
					byte[] data = zkService.getData(path);
					if (data != null && data.length > 0) {
						node.setContent(new String(data, "utf-8"));
					}
					node.setExisted(true);
					childrenNodes.add(node);
				}
				parent.setChildren(childrenNodes);
				for (ZkNode node : childrenNodes) {
					loadChildrenRecursively(node);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return childrenNodes;
	}

	private List<ZkNode> loadBusinessConf(String dsName, String dsType) throws Exception {
		ZkNode root = initialTreeOfPath(DBUS_CONF_TEMPLATE_ROOT, false);
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
						if (parentNode.getChildren() == null)
							parentNode.setChildren(new ArrayList<>());
						parentNode.getChildren().add(zkNode);
					}
					parentPath = currentNodePath;
					parentNode = parentNode.getChildByName(nodeName);
				}
				//  最末级节点，也就是退出for循环时，parentNode代表的节点，是我们根据dsType筛出来的，要加载的子树的root。根据这个root，加载子树
				parentNode.setChildren(loadChildrenRecursively(parentNode));
			}
		}

		// 上述步骤结束后，生成了根据不同dsType，需要加载的conf template子孙节点级。进一步处理templateRoot的子（孙—）节点。
		List<ZkNode> confTemplateChildren = templateRoot.getChildren();
		// 根据dsName把配置模板渲染成业务配置节点
		if (confTemplateChildren != null && !confTemplateChildren.isEmpty()) {
			for (ZkNode node : confTemplateChildren) {
				renderBusinessTreeBasedTemplate(node, dsName);
			}
		}
		return confTemplateChildren;
	}

	private void renderBusinessTreeBasedTemplate(ZkNode zkNode, String dsName) {
		zkNode.setName(zkNode.getName().replace(KeeperConstants.BUSSINESS_PLACEHOLDER, dsName));
		String path = zkNode.getPath();
		path = path.replace(TEMPLATE_NODE_NAME, "");
		path = path.replace(KeeperConstants.BUSSINESS_PLACEHOLDER, dsName);
		zkNode.setPath(path);
		try {
			// 检查是否存在与dsName对应的业务配置节点，设置标志。并且，存在的话，获取具体配置内容
			boolean exists = zkService.isExists(path);
			zkNode.setExisted(exists);
			if (exists) {
				byte[] data = zkService.getData(path);
				if (data != null && data.length > 0) {
					zkNode.setContent(new String(data, "utf-8"));
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		List<ZkNode> children = zkNode.getChildren();
		if (children != null && !children.isEmpty()) {
			for (ZkNode node : children) {
				renderBusinessTreeBasedTemplate(node, dsName);
			}
		}
	}


	private List<ZkNode> loadNodeRecursively(ZkNode nodeOfPath, int level) throws Exception {
		List<ZkNode> childrenNode = new ArrayList();
		try {
			List<String> children = null;
			String path = nodeOfPath.getPath();
			if (!zkService.isExists(path)) {
				logger.info("节点:{}不存在.", path);
				return null;
			}
			children = zkService.getChildren(path);
			if (children.size() > 0) {
				Collections.sort(children, (s1, s2) -> s1.compareToIgnoreCase(s2));
					/* Collections.sort(children, new Comparator() {
						public int compare(Object o1, Object o2) {
							String s1 = (String)o1;
							String s2 = (String)o2;
							return s1.compareToIgnoreCase(s2) > 0 ? 1:-1;
						}
					});*/
				for (String child : children) {
					ZkNode node = new ZkNode(child);
					String childPath = null;
					if (path.equals("/")) {
						childPath = path + child;
					} else {
						childPath = path + "/" + child;
					}
					node.setPath(childPath);
					node.setStat(zkService.getStat(path));
					node.setAcl(zkService.getACL(path));
					childrenNode.add(node);
					//System.out.println("loadding zkNode......." + node.getPath());
					if (level > 0) {
						loadNodeRecursively(node, level - 1);
					}
				}
				nodeOfPath.setChildren(childrenNode);
			}
			return childrenNode;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return childrenNode;
		}
	}


	/**
	 * 递归删除给定路径的zk结点
	 */
	private String deleteNodeRecursively(String path) throws Exception {
		List<String> children = null;
		children = zkService.getChildren(path);
		String result = null;
		if (children.size() > 0) {
			for (String child : children) {
				String childPath = null;
				if (path.equals("/")) {
					childPath = path + child;
				} else {
					childPath = path + "/" + child;
				}
				result = deleteNodeRecursively(childPath);
			}
		}
		//for (String confFilePath : InitZooKeeperNodesTemplate.ZK_PROTECT_NODES_PATHS) {
		//	if (path.equals(confFilePath)) {
		//		return confFilePath + "是受保护的节点,不能删除";
		//	}
		//}

		if (zkService.getChildren(path) != null && zkService.getChildren(path).size() > 0) {
			return result;
		}
		zkService.deleteNode(path);
		return null;
	}


	private void cloneNodeRecursively(String templatePath, String dsName, String dsType) {
		try {
			List<String> children = zkService.getChildren(templatePath);
			if (children.size() > 0) {
				for (String nodeName : children) {
					String templateNodePath = templatePath + "/" + nodeName;
					//判断节点是否存在，如果不存在，则创建
					String businessNodePath = templateNodePath.replace(TEMPLATE_NODE_NAME, "");
					businessNodePath = businessNodePath.replaceAll("typePlaceholder", dsType);
					businessNodePath = businessNodePath.replaceAll("placeholder", dsName);
					if (!zkService.isExists(businessNodePath)) {
						// 公共节点可能被重复处理，例如：/DBus/ConfTemplaes/Topology，可能被处理多次。
						// 但结果幂等。为了代码简洁性，选择接受重复处理，而不是各种判断，特殊处理，增加代码复杂性,导致代码不可读。
						initialTreeOfPath(businessNodePath, true);
					}
					cloneNodeRecursively(templateNodePath, dsName, dsType);
				}
			} else {
				populateLeafNodeData(templatePath, dsName, dsType);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void populateLeafNodeData(String templatePath, String dsName, String dsType) {
		String businessNodePath = templatePath.replace(TEMPLATE_NODE_NAME, "");
		businessNodePath = businessNodePath.replaceAll("typePlaceholder", dsType);
		businessNodePath = businessNodePath.replaceAll("placeholder", dsName);  //替换占位符placeholder为dsName
		try {
			if (zkService.isExists(businessNodePath)) {
				byte[] data = zkService.getData(templatePath);
				String nodeData = null;
				if (data != null && data.length > 0) {
					nodeData = new String(zkService.getData(templatePath), "UTF-8");
				}
				if (nodeData != null) {
					nodeData = nodeData.replaceAll("typePlaceholder", dsType);
					nodeData = nodeData.replaceAll("placeholder", dsName);
				} else {
					nodeData = "";
				}
				zkService.setData(businessNodePath, nodeData.getBytes(KeeperConstants.UTF8));
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public Properties loadZKNodeProperties(String path) throws Exception {
		if(!zkService.isExists(path)){
			return null;
		}
		return zkService.getProperties(path);
	}

	public ZkNode loadZKNodeJson(String path, String admin) throws Exception {
		if(!zkService.isExists(path)){
			return null;
		}
		byte[] data = zkService.getData(path);
		ZkNode zkNode = new ZkNode();
		zkNode.setStat(zkService.getStat(path));
		zkNode.setAcl(zkService.getACL(path));
		zkNode.setPath(path);
		if (null != data) {
			zkNode.setContent(new String(data, KeeperConstants.UTF8));
		}
		if (Constants.MYSQL_PROPERTIES_ROOT.equals(path) && admin == null) {
			zkNode.setContent("");
		}
		return zkNode;
	}

	public void updateZKNodeJson(String path, String content) throws Exception {
		if (!Constants.MYSQL_PROPERTIES_ROOT.equals(path)) {
			zkService.setData(path, content.getBytes(KeeperConstants.UTF8));
		}
	}

	public void updateZKNodeProperties(String path, Map<String, Object> map) throws Exception {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			sb.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
		}
		zkService.setData(path, sb.toString().getBytes(KeeperConstants.UTF8));
	}
}
