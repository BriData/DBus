var React = require('react');
var Cell = require('fixed-data-table').Cell;
var ReactDOM = require('react-dom');
var B = require('react-bootstrap');
var $ = require('jquery');
var utils = require('../common/utils');
var content = $("#root>.App.content");
var List = React.createClass({
    componentDidMount: function() {
        var self = this;
        $(document.body).one("click", function(e){
            if($(e.target).is(".select")) {
                return;
            }
            self._close();
        });
    },
    _clickWrapper: function(data) {
        var self = this;
        return function(e) {
            e.preventDefault();
            self._itemClick(data, e);
        }
    },
    _createList: function(dataList, fixed, selectedJar) {
        var list = [];
        var self = this;
        dataList.forEach(function(data, i) {
            var text = typeof(data) === "string" ? data : data.text;
            if(text == fixed) {
                text = (<div className="select" style={{color:"#009966"}}><span className="glyphicon glyphicon-ok-circle"></span> {text}</div>);
            } else if(text == selectedJar){
                text = (<div className="select" style={{color:"#999966"}}><span className="glyphicon glyphicon-ok"></span> {text}</div>);
            } else {
                text = (<div className="select" style={{paddingLeft:14}}>{text}</div>);
            }
            list.push(<li><a className="select" href="#" key={i} onClick={self._clickWrapper(data)}>{ text }</a></li>);
        });
        return list;
    },
    _itemClick: function(data, e) {
        e.preventDefault();
        console.log(data);
        this.props.itemSelect(data);
        this._close();
        return false;
    },
    _close: function() {
        $(ReactDOM.findDOMNode(this)).remove();
    },
    render: function() {
        return (
            <div className="open">
                <ul style={this.props.pos}
                    role="menu"
                    className="dropdown-menu select-pannel"
                    aria-labelledby="dropdown-no-caret"
                    data-reactid={901}>
                    {this._createList(this.props.data, this.props.fixed, this.props.selectedJar)}
                </ul>
            </div>
        );
    }
});

var SelectCell = React.createClass({
    _getData: function(data, rowIndex) {
        return data && data[rowIndex]? data[rowIndex]: null;
    },
    chooseOthers: function(e) {
        e.preventDefault();
        var dom = $(ReactDOM.findDOMNode(this));
        var pos = {
            top: dom.offset().top + dom.height(),
            left: dom.offset().left - 2,
            width:dom.width() + 3
        };
        var data = this._getData(this.props.data, this.props.rowIndex);
        console.log(data);
        ReactDOM.render(<List pos={pos} data={data.jarList} fixed={data.jarName} selectedJar={data.selectedJar} itemSelect={this.onChange}/>, document.getElementById("x"));
    },
    onChange: function(selected) {
        var data = this._getData(this.props.data, this.props.rowIndex);
        this.props.onSelect(data, selected);
    },
    render: function() {
        var data = this._getData(this.props.data, this.props.rowIndex);
        var label = "";
        if(data) {
            label = data[this.props.col];
        }
        var opts = utils.extends({}, this.props);
        delete opts.col;
        return (
            <div className="cell selectCell">
                {React.createElement(Cell, opts, label)}
                <a href="#" onClick={this.chooseOthers}><div>...</div></a>
            </div>
        );
    }
});

module.exports = {
    SelectCell: SelectCell
};
