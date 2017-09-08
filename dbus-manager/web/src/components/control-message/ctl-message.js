var React = require('react');
var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var store = require('./ctlmsg-store');
var JSONEditor = require('jsoneditor');
var $ = require('jquery');

var Form = B.Form;
var Col = B.Col;
var FormGroup = B.FormGroup;
var FormControl = B.FormControl;
var ControlLabel = B.ControlLabel;
var Button = B.Button;

var editor = null;
var Modal = B.Modal;

var ControlMessage = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    _onStatusChange: function(state) {
        this.setState(state);
        if(editor)
            editor.set(state.editor.json);
    },
    componentDidMount: function() {
        store.actions.initialLoad();
        this.initJsonEditor({});
    },
    initJsonEditor: function(value) {
        var container = this.refs.jsoneditor;
        var options = {
            sortObjectKeys: true,
            search: false,
            mode: 'view',
            modes: ['code', 'view']
        };
        editor = new JSONEditor(container, options);
        editor.set(value);


    },
    createDsList: function() {
        var list = [];
        if(this.state.dsOptions){
          this.state.dsOptions.forEach(function(ds, idx) {
            list.push(<option key={idx} value={ds.value} >{ds.text}</option>);
          });
        }
        return list;
    },
    createMsgTypeList: function() {
        var list = [];
        if(this.state.msgTypeOpts){
          this.state.msgTypeOpts.forEach(function(t, idx) {
            list.push(<option key={idx} data={t.template} value={t.type}>{t.text}</option>);
          });
        }
        return list;
    },
    messageTypeChanged: function() {
        var messageType = ReactDOM.findDOMNode(this.refs.messageType).value;
        store.actions.messageTypeChanged(messageType);
        if(messageType == "HEARTBEAT_RELOAD_CONFIG"){
          this.refs.dsDiv.style.display = "none";
          this.refs.ctrlDiv.style.display = "none";
        }else{
          this.refs.dsDiv.style.display = "";
          this.refs.ctrlDiv.style.display = "";
        }
    },
    sendMessage: function(e) {
        var btn = $(e.target);
        btn.attr("disabled", true);
        var ds = ReactDOM.findDOMNode(this.refs.ds).value;

        var ctrlTopic = ReactDOM.findDOMNode(this.refs.ctrlTopic).value;
        var messageType = ReactDOM.findDOMNode(this.refs.messageType).value;
        if(messageType != "HEARTBEAT_RELOAD_CONFIG"){
          if(ds == -1) {
            alert("请选择数据源");
            btn.removeAttr('disabled');
            return;
          }
        }
        if(messageType == -1) {
            alert("请选择消息类型");
            btn.removeAttr('disabled');
            return;
        }
        var message = editor.get();
        store.actions.sendMessage(ds,ctrlTopic,message, messageType,function(msg) {
            btn.removeAttr('disabled');
            alert(msg);
        });
    },
    datasourceChanged:function(){
        var dsId = ReactDOM.findDOMNode(this.refs.ds).value;
        var dsName = [];
        this.state.dsOptions.forEach(function(ds, idx) {
            if(ds.value == dsId){
               var dsText = ds.text;
               dsName = dsText.split("/");
            }
        });
        ReactDOM.findDOMNode(this.refs.ctrlTopic).value = dsName[1] + "_ctrl";
    },
    closeDialogZK:function(){
        store.actions.closeDialogZK();
    },
    readZkNode:function(){
        //this.props.history.pushState({}, "/zk-manager");
        var messageType = ReactDOM.findDOMNode(this.refs.messageType).value;
        store.actions.readZkNode(messageType,this);
    },
    render: function() {
        return (
            <div className="container-fluid">
                <div className="row header">
                    <h4 className="col-xs-12">
                        Control message
                    </h4>
                </div>
                <div className="row text-center">
                </div>
                <div className="row body">
                    <Form horizontal>
                        <FormGroup>
                            <Col componentClass={ControlLabel} sm={2}>
                                message type
                            </Col>
                            <Col sm={4}>
                                <FormControl
                                    ref="messageType"
                                    onChange={this.messageTypeChanged}
                                    componentClass="select">
                                    <option value="-1">select message type</option>
                                    { this.createMsgTypeList() }
                                </FormControl>
                            </Col>
                        </FormGroup>
                       <div ref="dsDiv" style={{display: ""}}>
                        <FormGroup>                            
                            <Col componentClass={ControlLabel} sm={2}>
                                data source
                            </Col>
                            <Col sm={4}>
                                
                                <FormControl
                                    ref="ds"
                                    onChange={this.datasourceChanged}
                                    componentClass="select">
                                    <option value="-1" >select data source</option>
                                    { this.createDsList() }
                                </FormControl>
                            </Col>          
                        </FormGroup>
                        </div>

                        <div ref="ctrlDiv" style={{display: ""}}>
                         <FormGroup>
                            <Col componentClass={ControlLabel} sm={2}>
                                ctrl_topic
                            </Col>
                            <Col sm={4}>
                                <FormControl
                                    ref="ctrlTopic"
                                    componentClass="input">
                                </FormControl>
                            </Col>
                        </FormGroup>
                        </div>
                       
                        <FormGroup>
                            <Col smOffset={2} sm={6}>
                                <div ref="jsoneditor" style={{height:210}}></div>
                            </Col>
                        </FormGroup>
                        <FormGroup>
                            <Col smOffset={2} sm={6}>
                                <Button type="button" bsStyle="primary" onClick={this.sendMessage}>
                                    <span className="glyphicon glyphicon-send"></span> Send Control Message
                                </Button>
                            </Col>
                        </FormGroup>

                        <FormGroup>
                            <Col smOffset={2} sm={6}>
                                <Button type="button" bsStyle="primary" onClick={this.readZkNode}>
                                    <span className="glyphicon glyphicon-book"></span> Read ZK Node
                                </Button>
                            </Col>
                        </FormGroup>


                    </Form>
                </div>
                <div id="dialog">
                    <Modal
                            show={this.state.dialogCtrl.showJson}
                            onHide={this.closeDialogZK}>
                            <Modal.Header closeButton>
                                <Modal.Title>{this.state.dialogCtrl.identityJson}</Modal.Title>
                            </Modal.Header>
                            <Modal.Body>
                                 <textarea  ref="dataResult"  rows="10" cols="80" style={{resize:"none"}}></textarea>
                            </Modal.Body>
                            <Modal.Footer>
                                <B.Button onClick={this.closeDialogZK}>Close</B.Button>
                            </Modal.Footer>
                        </Modal>
                </div>
            </div>
        );
    }
});

module.exports = ControlMessage;
