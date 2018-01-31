var React = require('react');
var styles  = require('./styles');
var StyleRoot  = require('radium').StyleRoot;
var $ = require('jquery');
var utils = require('../utils');
var ReactDOM = require('react-dom');
import {Form, Tabs, Input, Button} from 'antd';
const FormItem = Form.Item;
const TabPane = Tabs.TabPane;
const Textarea = Input.TextArea;

const formItemLayout = {
    labelCol: {
        xs: { span: 24 },
        sm: { span: 6 },
    },
    wrapperCol: {
        xs: { span: 24 },
        sm: { span: 14 },
    },
};

var NodeViewer  = React.createClass({
    getInitialState: function() {
        return {
            current: 'nodeData',

        }
    },

    componentWillReceiveProps: function (props) {
        //console.log('A componentDidUpdate');
        if(props.node && props.node.currentNodeData) {
            this.state.czxid = props.node.currentNodeData.czxid;
            this.state.mzxid = props.node.currentNodeData.mzxid;
            this.state.ctime = props.node.currentNodeData.ctime;
            this.state.mtime = props.node.currentNodeData.mtime;
            this.state.version = props.node.currentNodeData.version;
            this.state.cversion = props.node.currentNodeData.cversion;
            this.state.aversion = props.node.currentNodeData.aversion;
            this.state.ephemeralOwner = props.node.currentNodeData.ephemeralOwner;
            this.state.dataLength = props.node.currentNodeData.dataLength;
            this.state.numChildren = props.node.currentNodeData.numChildren;
            this.state.pzxid = props.node.currentNodeData.pzxid;
            this.state.permission = props.node.currentNodeData.permission;
            this.state.scheme = props.node.currentNodeData.scheme;
            this.state.id = props.node.currentNodeData.id;
            //console.log(this.state.permission);
        }
    },

    modifyZkNodeContent:function(){
        var self=this;
        let nodeContent = ReactDOM.findDOMNode(this.refs.nodeContent).value;
        var formdata={
            path:this.props.node.currentNodePath,
            data:nodeContent
        };
        console.log(formdata);
        $.post(utils.builPath("zk/modifyZkNodeData"), formdata, function(result) {
            if(result.status !== 200) {
                alert("修改失败！");
                return;
            }
            // 更新前端“缓存”的state数据
            ReactDOM.findDOMNode(self.refs.nodeContent).value=formdata.data;
            //self.props.node.content=formdata.data;
            //self.props.onChange(self.props.node);
            alert("修改成功");
        });
    },

    render:function () {
      var content = '';
      var nodePath = '';
      if (this.props.node) {
        nodePath = this.props.node.currentNodePath;
        content = this.props.node.currentNodeData.content;
      }
      var nodeContentTexArea=ReactDOM.findDOMNode(this.refs.nodeContent);
      if(nodeContentTexArea) {
        ReactDOM.findDOMNode(this.refs.nodeContent).value = content;
      }
      //<Button type="primary" icon="save" onClick={this.modifyZkNodeContent}>Save </Button>
         return (<StyleRoot>
            <Tabs type="card" defaultActiveKey=".$nodeData"  >
                <TabPane tab="Node Data" key="nodeData">
                        <div  className="saveNodeButton">
                            <div className="col-sm-offset-2 col-sm-10">
                                <button type="button" className="btn btn-default btn-sm" onClick={this.modifyZkNodeContent}>
                                    <span className="glyphicon glyphicon-floppy-save" style={{ marginRight: '5px', color:'#8096d2'}}/>Save</button>
                            </div>
                        </div>
                        <Textarea ref="nodeContent" className="col-sm-10 col-sm-12" rows="18" style={styles.nodeContentTextarea}></Textarea>
                </TabPane>
                <TabPane tab="Node Meta Data" key="nodeMeta">
                    <div className="paneView">
                    <Form className="horizontal">
                        <FormItem {...formItemLayout} >
                        <label>czxid:   </label>
                        <input type="text" className="nodeInfoInput" ref="czxid" value={this.state.czxid} disabled="disabled"></input>
                    </FormItem>
                        <FormItem {...formItemLayout}>
                            <label>mzxid:   </label>
                            <input type="text" className="nodeInfoInput" ref="mzxid" value={this.state.mzxid} disabled="disabled"></input>
                        </FormItem>
                        <FormItem {...formItemLayout}>
                            <label>ctime:   </label>
                            <input type="text" className="nodeInfoInput" ref="ctime"  value={this.state.ctime}  disabled="disabled" ></input>
                        </FormItem>
                        <FormItem {...formItemLayout} >
                            <label>mtime:   </label>
                            <input type="text" className="nodeInfoInput" ref="mtime"  value={this.state.mtime} disabled="disabled"></input>
                        </FormItem>
                        <FormItem {...formItemLayout}>
                            <label>version:   </label>
                            <input type="text" className="nodeInfoInput" ref="version"  value={this.state.version}  disabled="disabled"></input>
                        </FormItem>
                        <FormItem {...formItemLayout}>
                            <label>cversion:   </label>
                            <input type="text" className="nodeInfoInput" ref="cversion"  value={this.state.cversion}  disabled="disabled" ></input>
                        </FormItem>
                        <FormItem {...formItemLayout} >
                            <label>aversion:   </label>
                            <input type="text" className="nodeInfoInput" ref="aversion" value={this.state.aversion}  disabled="disabled"></input>
                        </FormItem>
                        <FormItem {...formItemLayout}>
                            <label>ephemeralOwner:   </label>
                            <input type="text" className="nodeInfoInput" ref="ephemeralOwner" value={this.state.ephemeralOwner}   disabled="disabled"></input>
                        </FormItem>
                        <FormItem {...formItemLayout}>
                            <label>dataLength:   </label>
                            <input type="text" className="nodeInfoInput" ref="dataLength" value={this.state.dataLength}  disabled="disabled" ></input>
                        </FormItem>
                        <FormItem {...formItemLayout}>
                            <label>numChildren:   </label>
                            <input type="text" className="nodeInfoInput" ref="numChildren" value={this.state.numChildren}  disabled="disabled"></input>
                        </FormItem>
                        <FormItem {...formItemLayout}>
                            <label>pzxid:   </label>
                            <input type="text" className="nodeInfoInput" ref="pzxid"  value={this.state.pzxid} disabled="disabled" ></input>
                        </FormItem>
                    </Form>
                    </div>
                </TabPane>
                <TabPane tab="Node ACLs" key="nodeAcl">
                    <div className="paneView">
                    <Form ref="nodeACLs">
                        <FormItem  {...formItemLayout} >
                            <label>Scheme:      </label>
                            <input type="text" className="nodeInfoInput" ref="scheme" value={this.state.scheme}  disabled="disabled" ></input>
                        </FormItem>
                        <FormItem  {...formItemLayout}>
                            <label>Id:      </label>
                            <input type="text" className="nodeInfoInput" ref="id" value={this.state.id}  disabled="disabled" ></input>
                        </FormItem>
                        <FormItem  {...formItemLayout}>
                            <label>Permissions:       </label>
                            <input type="text" className="nodeInfoInput" ref="permission" value={this.state.permission}  disabled="disabled" ></input>
                        </FormItem>
                    </Form>
                    </div>
                </TabPane>
            </Tabs>
        </StyleRoot>);
    }
});

NodeViewer.propTypes = {
    node: React.PropTypes.object
};

module.exports = NodeViewer;
