var React = require('react');
var utils = require('./utils');
var Modal = require('react-bootstrap').Modal;
var $ = require('jquery');
var ReactDOM = require('react-dom');

var Loading = React.createClass({
    getInitialState: function() {
        return {
            showModal: false,
            text: "Data Loading...",
            icon: 'icon'
        };
    },
    componentDidMount: function() {
        utils.loading = this;
    },
    render: function() {
        return (
            <Modal ref="global-loading" keyboard={true} backdrop={true} bsSize="small" show={this.state.showModal} dialogClassName="loading">
                <Modal.Body>
                    <i className="icon-spinner icon-spin"></i> {this.state.text}
                </Modal.Body>
            </Modal>
        );
    },
    show: function (text, iconClassName) {
        text = text || "Data Loading..."
        iconClassName = iconClassName || "icon-spinner icon-spin"
        this.setState({ showModal: true, text: text, icon: iconClassName ? 'icon' + iconClassName: 'icon' });
    },
    hide: function() {
        this.setState({ showModal: false, text: "Data Loading...", icon: "icon-spinner icon-spin" });
    }
});

module.exports = Loading;
