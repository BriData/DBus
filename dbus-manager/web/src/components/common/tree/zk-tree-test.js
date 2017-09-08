/**
 * Created by ximeiwang on 2017/7/18.
 */
var React = require('react');
var $ = require('jquery');
var Reflux = require('reflux');
var styles  = require('./styles');
var StyleRoot  = require('radium').StyleRoot;
var NodeViewer  = require('./node-view');
var store = require('./zk-tree-store');
var FreeScrollBar = require('react-free-scrollbar');
var utils = require('../utils');

import { Tree,Modal} from 'antd';
const TreeNode = Tree.TreeNode;
const ContextMenu = require('react-contextmenu').ContextMenu;
const MenuItem = require('react-contextmenu').MenuItem;
const ContextMenuTrigger = require('react-contextmenu').ContextMenuTrigger;

const confirm = Modal.confirm;
const CreateNodePage = require('./addNodeModal');

const getExpandedKeys = (searchValue, dataList, expandedKeys) => {
    dataList.map((item)=>{
        if (item.name.toLowerCase().indexOf(searchValue.toLowerCase()) > -1) {
            let parentPath = item.path;
            while(parentPath.lastIndexOf('/')  > 0 ){
                parentPath = parentPath.substring(0, parentPath.lastIndexOf('/'));
                expandedKeys.add(".$" + parentPath);
            }
        }
        if(item.children){
            return getExpandedKeys(searchValue,item.children,expandedKeys);
        }else{
            return null;
        }
    });
};
var ZkTreeTest = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    _onStatusChange: function(state) {
        this.setState(state);
    },

    componentDidMount: function() {
        // 监听select-all事件，在CheckboxCell中会发出select-all事件
        store.actions.initialLoad();
    },

    OnSelected: function(selectedKeys, e){
        console.log(selectedKeys);
        let currentNodePath = "";
        if(e.selected) {
            currentNodePath = selectedKeys[0].replace(".$","");
            store.actions.getNodeInfo(currentNodePath);
        }
        //this.setState({selectedKeys:selectedKeys});
        store.actions.setSelectedKeys(selectedKeys);
    },
    onExpand:function(expandedKeys, node) {
        let path = node.node.props.eventKey.replace(".$","");
        if(node.expanded == true){
            //this.setState({expandedKeys: expandedKeys,});
            store.actions.setExpandedKeys(expandedKeys);
            this.props.loadChildNode(path);
        }else{
            expandedKeys = expandedKeys.filter((item) =>{
                return (item.indexOf(path)) !== 2
            });
            //this.setState({ expandedKeys: [...expandedKeys], });
            store.actions.setExpandedKeys(expandedKeys);
        }
    },
    showAddModal: function() {
        this.refs.addModal.showModal();
    },
    addNode:function(title){
        store.actions.addNode(this.state.currentNode, title, this.loadChildNode);
    },
    deleteNode:function(){
        const currentNode = this.state.currentNode;
        this.props.deleteNode(currentNode);
    },
    deleteConfirm:function(){
        const currentNode = this.state.currentNode;
        let deleteNode = this.deleteNode;
            confirm({
                title: 'Do you Want to delete this node?',
                onOk() {
                    deleteNode();
                },
                onCancel() {
                    console.log('Cancel');
                },
            });
    },
    loadChildNode:function() {
        const currentNode = this.state.currentNode;
        this.props.loadChildNode(currentNode);
    },

render: function() {
        var data=this.state.data||this.props.data||'';
        const iconClass =  ['glyphicon glyphicon-folder-open','glyphicon glyphicon-list-alt'];
        const iconStyle =  [{ marginRight: '5px', color:'#f5b041'},{ marginRight: '5px', color:'#7f8c8d'}];
        const loop = data => data.map((item) => {
            if (item.children) {
                return <TreeNode title={<span>{item.name}</span>} key={item.path} isLeaf={false}>
                    { loop(item.children) }
                </TreeNode>;
            }else{
                return <TreeNode title={<span>{item.name}</span>} key={item.path} isLeaf={true}></TreeNode>;
            }
        });
        if(data){
            let arrayData = [];
            arrayData.push(data);
            return (
            <StyleRoot>
                <div style={styles.scrolledContainer}>
                    <FreeScrollBar>
                        <ContextMenuTrigger id="folderNode">
                        <Tree showLine showIcon onExpand={this.onExpand}  defaultExpandedKeys={this.state.defaultExpandedKeys} expandedKeys={this.state.expandedKeys} onSelect={this.OnSelected} onRightClick={this.onRightClick}>
                            {loop(arrayData)}
                        </Tree>
                        </ContextMenuTrigger>
                        <ContextMenu id="folderNode">
                            <MenuItem ref="addNode" data={"addNode"} onClick={this.showAddModal} >
                                <span className="glyphicon glyphicon-plus"></span>  <span className="contextMenuItem">添加结点</span>
                           </MenuItem>
                            <MenuItem data={"deleteNode"} onClick={this.deleteConfirm} >
                                <span className="glyphicon glyphicon-remove"></span>  <span className="contextMenuItem">删除结点</span>
                            </MenuItem>
                        </ContextMenu>
                        <CreateNodePage ref="addModal" onSave={this.addNode}></CreateNodePage>
                    </FreeScrollBar>
                </div>
                <div className="viewer"><NodeViewer node={this.state.currentNodeInfo}/></div>
            </StyleRoot>)
        }else{
            return (<div>Tree is Empty!</div>);
        }
    },

    onRightClick:function({event, node}) {
        //this.setState({currentNode: node.props.eventKey.replace(".$","")});
        store.actions.setCurrentNode(node.props.eventKey.replace(".$",""));
    },

});
module.exports = ZkTreeTest;
