/**
 * Created by ximeiwang on 2017/8/18.
 */
var React = require('react');
var styles  = require('./styles');
var StyleRoot  = require('radium').StyleRoot;
var $ = require('jquery')
var utils = require('../utils');
var ReactDOM = require('react-dom');
var NodeViewerZkTree  = React.createClass({
    modifyZkNodeContent:function(){
        var self=this;
        var formdata={
            path:this.props.node.path,
            data:ReactDOM.findDOMNode(this.refs.nodeContent).value
        }
        $.post(utils.builPath("zk/modifyZkNodeData"), formdata, function(result) {
            if(result.status !== 200) {
                alert("修改失败！");
                return;
            }
            // 更新前端“缓存”的state数据
            ReactDOM.findDOMNode(self.refs.nodeContent).value=formdata.data;
            self.props.node.content=formdata.data;
            self.props.onChange(self.props.node);
            alert("修改成功");
        });
    },
    refreshContent:function(){
        var self=this;
        var curNode=this.props.node;
        var refreshSubTree = ReactDOM.findDOMNode(this.refs.refreshSubTree).checked;
        $.get(utils.builPath("zk/loadNodeAndDirectChildren"), {path:curNode.path,refreshSubTree:refreshSubTree}, function(result) {
            if(result.status !== 200) {
                // self.state.data = [{name:'加载失败！'}];
                return;
            }
            ReactDOM.findDOMNode(self.refs.nodeContent).value=result.data.content;
            self.props.node.content=  result.data.content;
            if(refreshSubTree&&result.data.children){
                self.props.node.children=  result.data.children;
            }
            self.props.onChange(self.props.node);
        });
    },
    render:function () {
        var content = '';
        var nodePath = '';
        if (this.props.node) {
            content = this.props.node.content;
            nodePath = this.props.node.path;
        }
        var nodeContentTexArea=ReactDOM.findDOMNode(this.refs.nodeContent);
        if(nodeContentTexArea) {
            ReactDOM.findDOMNode(this.refs.nodeContent).value = content;
        }//
        return (<StyleRoot>
            <div><span style={styles.zkTextareaPathTitle}>{nodePath}</span></div>
            <div style={{marginBottom:'4px', marginTop:'4px'}}>
                <button type="button" className="btn btn-default btn-sm" onClick={this.modifyZkNodeContent}
                        style={styles.nodeContentModifyBtn}>修 改</button>
                <button type="button" className="btn btn-default btn-sm" onClick={this.refreshContent}
                        style={styles.nodeContentRefreshBtn}>刷 新</button>
                <input ref="refreshSubTree" type="checkbox" style={styles.subtreeRefreshCheckBox}/><span style={{color: 'black'}}>同时刷新子树</span>

            </div>
            <textarea className="col-sm-10 col-sm-12" rows="36" ref="nodeContent"
                      style={styles.nodeContentTextarea}></textarea>
        </StyleRoot>);
    }
});

NodeViewerZkTree.propTypes = {
    node: React.PropTypes.object
};

module.exports = NodeViewerZkTree;