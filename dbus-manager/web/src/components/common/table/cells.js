var React = require('react');
var Cell = require('fixed-data-table').Cell;
var ReactDOM = require('react-dom');
var utils = require('../utils');
var B = require('react-bootstrap');
var A = require('antd');

var getData = function(data, idx) {
    return data?data[idx]:null;
};

var _getData = function(idx, data, key, others) {
    try {
        if(data && data[idx]) {
            return data[idx][key];
        } else {
            return null;
        }
    } catch(e) {
        if(console) console.error(e);
        return null;
    }
};

var TextCell = React.createClass({
    _getStyle: function() {
        return { width:"99%", overflowX: "hidden" };
    },
    _dblClick: function() {
        var cb = this.props.onDoubleClick
        if(cb) cb(getData(this.props.data, this.props.rowIndex));
    },
    render: function() {
        var opts = utils.extends({}, this.props);
        delete opts.col;
        delete opts.onDoubleClick;

        var data = _getData(this.props.rowIndex, this.props.data, this.props.col, this.props);
        var cell = React.createElement(Cell, opts, data);

        var divOpts = {style: this._getStyle()};
        var cb = this.props.onDoubleClick;
        if(cb && typeof cb == "function") {
            divOpts.onDoubleClick = this._dblClick;
            divOpts.style.cursor = "pointer";
        }
        return React.createElement("div", divOpts, cell);
    }
});

var ATTR_CKECKED = "__ckbox_checked__";
var DISABLED = "__disabled__";
var CheckboxCell = React.createClass({
    getInitialState: function() {
        return {
            checked:false
        };
    },
    _getStyle: function() {
        return {
            "marginRight": "5px"
        };
    },
    _onChange: function(e) {
        var props = this.props;
        var isChecked = this.refs.ck.checked;
        this.state.checked = isChecked;
        var data = this._getCkboxData(props.data, props.rowIndex);
        if(data) {
            data[ATTR_CKECKED] = isChecked;
            if(props.onChange) {
                props.onChange(isChecked, props.data? data: null);
            }
            if(props.emitter) {
                props.emitter.emit("select", e, isChecked);
            }
        } else if(props.data && Array.isArray(props.data)){
            // may be select all is called
            props.data.forEach(function(d){
                d[ATTR_CKECKED] = isChecked;
            });
            if(props.onChange) {
                props.onChange(isChecked, props.data? data: null);
            }
            if(props.emitter) {
                props.emitter.emit("selectAll", e, isChecked);
            }
        }

        return true;
    },
    _getCkboxData: function(data, rowIndex) {
        return data && data[rowIndex]? data[rowIndex]: null;
    },
    render: function() {
        var p = this.props;
        var checked = false;
        var disabled = false;
        var colName = "";
        var ckbox = null;
        if(!p.children) {
            var d = this._getCkboxData(p.data, p.rowIndex);
            if(!d) {
                return null;
            }
            colName = d[p.col];
            checked = d[ATTR_CKECKED];
            disabled = d[DISABLED];
        } else {
            checked = p.status;
        }
        return (
            <Cell className={ this.props.clazzName }>
                <label>
                    <input ref="ck" style={this._getStyle()} type="checkbox" onChange={this._onChange} checked= {checked} disabled = {disabled}/>
                    { p.children ? p.children: colName}
                </label>
            </Cell>
        );
    }
});

var LinkCell = React.createClass({
    _onLinkClick: function(e) {
        e.preventDefault();
        if(this.props.onClick) {
            this.props.onClick(this.props.data[this.props.rowIndex]);
        }
    },
    render: function() {
        var children = this.props.children;
        var content = (
            <a href="#" onClick={this._onLinkClick}>
                { children ? children: _getData(this.props.rowIndex, this.props.data, this.props.col, this.props)}
            </a>
        );
        var opts = utils.extends({}, this.props);
        delete opts.col;
        return React.createElement(Cell, opts, content);
    }
});

var BtnCell = React.createClass({
    _btnClick: function(callback, props, items) {
        return function(e, item, key, keyPath) {
            if(callback) {
                callback(getData(props.data, props.rowIndex), e);
            }
        };
    },
    _menuItemClick: function(callbackList, props) {
        return function(e) {
            var callback = callbackList[e.key];
            if(callback) {
                callback(getData(props.data, props.rowIndex), e);
            }
        };
    },
    render: function() {
        var btns = [];
        var self = this;

        var btnItems = null;
        var menuItems = [];
        if(this.props.btns.length > 3) {
            btnItems = this.props.btns.slice(0, 2);
            menuItems = this.props.btns.slice(2, this.props.btns.length);
        } else {
            btnItems = this.props.btns;
        }
        btnItems.forEach(function(btn, idx) {
            var content = [];
            if(btn.icon) {
                var iconClass = btn.icon?"glyphicon glyphicon-"+btn.icon:null;
                content.push(<span key={"icon-"+idx} className={iconClass}></span>);
            }
            content.push(" ", btn.text);

            var btnCompotent = null
            btns.push(
                <A.Button key={idx}
                    type={btn.bsStyle ? btn.bsStyle : "default"}
                    onClick={self._btnClick(btn.action, self.props)}>
                    {content}
                </A.Button>
            );
        });

        var items = [];
        var cbs = []
        menuItems.forEach(function(btn, idx) {
            var content = [];
            if(btn.icon) {
                var iconClass = btn.icon?"glyphicon glyphicon-"+btn.icon:null;
                content.push(<span key={"icon-"+idx} className={iconClass}></span>);
            }
            content.push(" ", btn.text);
            items.push(<A.Menu.Item key={idx}> {content} </A.Menu.Item>);
            cbs.push(btn.action);
        });
        if(items.length > 0) {
            var menu = <A.Menu onClick={self._menuItemClick(cbs, self.props)}>{items}</A.Menu>
            btns.push(<A.Dropdown overlay={menu}><A.Button>more <A.Icon type="down" /></A.Button></A.Dropdown>);
        }
        var content = (
            <div className="btn-cell">
                {btns}
            </div>
        );
        var opts = utils.extends({}, this.props);
        delete opts.btns;
        return React.createElement(Cell, opts, content);
    }
});
var StatusCell = React.createClass({
    _getStyle: function() {
        return { "width":"99%", "overflowX": "hidden" };
    },

    render: function() {
        var opts = utils.extends({}, this.props);
        delete opts.col;
        var styleGetter = this.props.styleGetter;
        var style;
        if(styleGetter) {
            style = styleGetter(getData(this.props.data, this.props.rowIndex));
        }
        var content = (
            <B.Label bsStyle={style ? style : "default"}>
                { _getData(this.props.rowIndex, this.props.data, this.props.col, this.props)}
            </B.Label>
        );
        delete opts.styleGetter;
        return React.createElement(Cell, opts, content);
    }
});
module.exports = {
    TextCell: TextCell,
    CheckboxCell: CheckboxCell,
    LinkCell: LinkCell,
    BtnCell: BtnCell,
    StatusCell: StatusCell
};
