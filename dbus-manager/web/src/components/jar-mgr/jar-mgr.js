var React = require('react');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var Tab = require('fixed-data-table');
var TF = require('../common/table/tab-frame');
var Select = require('../common/select');
var Table = require('../common/table/default-table');
var store = require('./jar-mgr-store');
var cells = require('../common/table/cells');
var utils = require('../common/utils');
import DropzoneComponent from 'react-dropzone-component';

var Column = Tab.Column;
var Cell = Tab.Cell;
var TextCell = cells.TextCell;
var Modal = B.Modal;

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
    openDialog:function(data){
        var id = data.id;
        var desc = data.desc;
        store.actions.openDialog(id,desc);
    },
    closeDialog: function(data) {
        var description = this.refs.description.value;
        store.actions.closeDialog(description);
    },
    render: function() {
        var rows = this.state.jarList || [];
        var componentConfig = this.state.componentConfig || [];
        var eventHandlers = this.state.eventHandlers || [];
        var djsConfig = this.state.djsConfig || [];
        return (
            <TF>
                <TF.Header title="Jar Manager" />
                <TF.Body >
                     <DropzoneComponent config={componentConfig}
                       eventHandlers={eventHandlers}
                       djsConfig={djsConfig} />
                    <Table rowsCount={rows.length}>
                        <Column
                            header={ <Cell>Path</Cell> }
                            cell={ <TextCell data={rows} col="path">appender.jar</TextCell>}
                            width={280} />
                        <Column
                            header={ <Cell>Description(double click on each to change description)</Cell> }
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
                            <Modal.Title>Description</Modal.Title>
                        </Modal.Header>
                        <Modal.Body>
                           <textarea className="form-control" ref="description"  rows="5" cols="120" style={{resize:"none"}}>{this.state.dialog.content}</textarea>
                        </Modal.Body>
                        <Modal.Footer>
                            <B.Button onClick={this.closeDialog}>OK</B.Button>
                        </Modal.Footer>
                </Modal>
            </TF>

        );
    }
});

module.exports = JarMgr;
