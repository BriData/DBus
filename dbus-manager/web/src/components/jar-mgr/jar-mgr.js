var React = require('react');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Table = require('../common/table/default-table');
var store = require('./jar-mgr-store');
var cells = require('../common/table/cells');
var utils = require('../common/utils');
var Select = require('../common/select');

var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;
var BtnCell = cells.BtnCell;
var Modal = B.Modal;

var antd = require('antd');

import { Upload, Icon, message, Button } from 'antd';
const Dragger = Upload.Dragger;

const props = {
    name: 'file',
    multiple: false,
    action: '/mgr/jarManager/upload',
    data: {JarUploadPath:""},
    beforeUpload: function () {
        console.log("JarMgr.getInitialState: " + JSON.stringify(store.state));
        if(store.state.JarUploadPath == "") {
            alert("请选择上传路径！");
            return false;
        }
        var vals = store.state.JarUploadPath.split("/");

        if(vals[1] == 0) {
            alert("请选择版本！");
            return false;
        }
        if(vals[2] == 0) {
            alert("请选择类型！");
            return false;
        }
        return store.state.JarUploadPath == "" ? false : true;
    },
    onChange(info) {
        const status = info.file.status;
        console.log("status: " + status);
        console.log("info: " + JSON.stringify(info));
        if (status !== 'uploading') {
            console.log(info.file, info.fileList);
        }
        if (status === 'done') {
            alert(info.file.name + "  文件上传成功！");
            // message.success(`${info.file.name} file uploaded successfully.`);
            store.actions.initialLoad();
        } else if (status === 'error') {
            alert(info.file.name + "  文件上传失败！");
            // message.error(`${info.file.name} file upload failed.`);
        }
    },
};

var JarMgr = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState(this);
        return state;
    },
    componentDidMount: function() {
        store.actions.initialLoad();
    },
    _onStatusChange: function(state) {
        this.setState(state);
    },
    openDialog: function(data) {
        var path = data.path;
        var desc = data.desc;
        console.log("path: " + path);
        store.actions.openDialog(path, desc);
    },
    closeDialog: function(data) {
        var description = this.refs.description.value;
        var path = this.state.dialog.path;
        store.actions.closeDialog(path, description);
    },

    onModalCancel: function () {
        store.actions.setVisible(false);
    },

    onModalOk: function () {
        store.actions.setVisible(false);
    },

    onUploadFile: function () {
        store.actions.setVisible(true);
    },
    
    deleteJar: function(param) {
        if(!confirm("Are you sure to delete this jar?")) return;
        utils.showLoading();
        store.actions.deleteJar(param);
    },

    dbusVersionSelected: function(dbusVersion) {
        console.log("dbusVersionSelected dbusVersion: " + JSON.stringify(dbusVersion));
        var dbusJarType = this.refs.dbusJarType.getValue();
        store.actions.dbusVersionSelected(dbusVersion, dbusJarType);
    },

    dbusJarTypeSelected: function(param) {
        console.log("dbusJarTypeSelected param: " + JSON.stringify(param));
        var dbusJarType = param;
        var dbusVersion = this.refs.dbusVersion.getValue();
        console.log("dbusJarTypeSelected this.refs.dbusVersion.text: " + JSON.stringify(dbusVersion));
        store.actions.dbusJarTypeSelected(dbusVersion, dbusJarType);
    },

    render: function() {
        var rows = this.state.jarList || [];
        props.data.JarUploadPath = this.state.JarUploadPath;
        console.log("props.data.JarUploadPath: " + props.data.JarUploadPath);
        if(props.data.JarUploadPath.indexOf("select a dbus version") != -1
            || props.data.JarUploadPath.indexOf("select a dbus jar type") != -1) alert("Jar upload path is invalid!");
        return (
            <TF>
                <TF.Header title={"JarUploadPath: " + this.state.JarUploadPath}>
                    <h4 className="col-xs-6">
                        <div style={{float:"right",width:"40px"}}>
                            <antd.Button icon="save" type="primary" onClick={this.onUploadFile}>Upload File</antd.Button>
                        </div>
                    </h4>
                </TF.Header>

                <TF.Body >
                    <Table rowsCount={rows.length}>
                        <Column
                            header={ <Cell>Operation</Cell> }
                            cell={<BtnCell data={rows}
                                    btns={[{text:"", bsStyle:"danger", icon:"glyphicon glyphicon-trash", action:this.deleteJar}]}/>}
                            width={80}/>
                        <Column
                            header={ <Cell>version</Cell> }
                            cell={ <TextCell data={rows} col="version"></TextCell>}
                            width={60} />
                        <Column
                            header={ <Cell>type</Cell> }
                            cell={ <TextCell data={rows} col="type"></TextCell>}
                            width={150} />
                        <Column
                            header={ <Cell>minor version</Cell> }
                            cell={ <TextCell data={rows} col="timestamp"></TextCell>}
                            width={150} />
                        <Column
                            header={ <Cell>jarName</Cell> }
                            cell={ <TextCell data={rows} col="jarName"></TextCell>}
                            width={350} />
                        <Column
                            header={ <Cell>description</Cell> }
                            cell={ <TextCell data={rows} col="desc" onDoubleClick={this.openDialog}/>}
                            width={300}
                            flexGrow={1}/>
                    </Table>
                </TF.Body>
                <Modal
                    bsSize="large"
                    show={this.state.dialog.show}
                    onHide={this.closeDialog}>
                    <Modal.Header closeButton>
                        <Modal.Title>{this.state.dialog.path}</Modal.Title>
                    </Modal.Header>
                    <Modal.Body>
                        <textarea className="form-control" ref="description"  rows="5" cols="120" style={{resize:"none"}}>{this.state.dialog.content}</textarea>
                    </Modal.Body>
                    <Modal.Footer>
                        <B.Button onClick={this.closeDialog}>OK</B.Button>
                    </Modal.Footer>
                </Modal>

                <antd.Modal title="upload file" visible={this.state.visible} onOk={this.onModalOk}
                            footer={[<Button key="submit" type="primary" onClick={this.onModalOk}> Return </Button>,]}
                            onCancel={this.onModalCancel}>
                    <div className="form-group">
                        <label for="selectStatus" className="col-sm-2 control-label">1. 版本:</label>
                        <Select
                            ref="dbusVersion"
                            defaultOpt={{value:0, text:"select a dbus version"}}
                            options={this.state.dbusVersion}
                            onChange={this.dbusVersionSelected}/>
                    </div>
                    <div className="form-group">
                        <label for="selectStatus" className="col-sm-2 control-label">2. 类型:</label>
                        <Select
                            ref="dbusJarType"
                            defaultOpt={{value:0, text:"select a dbus jar type"}}
                            options={this.state.dbusJarType}
                            onChange={this.dbusJarTypeSelected}/>
                    </div>
                    <div>
                        <Dragger {...props}>
                            <p className="ant-upload-drag-icon">
                                <Icon type="inbox" />
                            </p>
                            <p className="ant-upload-text">Click or drag file to this area to upload</p>
                            <p className="ant-upload-hint">Support for a single upload</p>
                        </Dragger>
                    </div>
                </antd.Modal>

            </TF>

        );
    }
});

module.exports = JarMgr;
