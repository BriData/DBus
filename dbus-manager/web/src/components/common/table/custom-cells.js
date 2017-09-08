var React = require('react');
var Cell = require('fixed-data-table').Cell;
var ReactDOM = require('react-dom');
var utils = require('../utils');
var B = require('react-bootstrap');
var getData = function (data, idx) {
    return data ? data[idx] : null;
};

var _getData = function (idx, data, key, others) {
    try {
        if (data && data[idx]) {
            return data[idx][key];
        } else {
            return null;
        }
    } catch (e) {
        if (console) console.error(e);
        return null;
    }
};


var BtnCell = React.createClass({
    _btnClick: function (callback, props) {
        return function (e) {
            if (callback) {
                callback(getData(props.data, props.rowIndex), e);
            }
        };
    },
    render: function () {
        var btns = [];
        var self = this;
        this.props.btns.forEach(function (btn, idx) {
            var content = [];
            if (btn.icon) {
                var iconClass = btn.icon ? "glyphicon glyphicon-" + btn.icon : null;
                content.push(<span key={"icon-"+idx} className={iconClass}></span>);
            }

            btns.push(
                <B.Button key={idx}
                          bsStyle={btn.bsStyle ? btn.bsStyle : "default"}
                          bsSize="small"
                          onClick={self._btnClick(btn.action, self.props)}
                          title={btn.text}>
                    {content}
                </B.Button>
            );
        });
        var content = (
            <B.ButtonToolbar>
                {btns}
            </B.ButtonToolbar>
        );
        var opts = utils.extends({}, this.props);
        delete opts.btns;
        return React.createElement(Cell, opts, content);
    }
});

var TextCell = React.createClass({
    _getStyle: function () {
        return {width: "99%", overflowX: "hidden", background: "#FFFFFF"};
    },
    _dblClick: function () {
        var cb = this.props.onDoubleClick
        if (cb) cb(getData(this.props.data, this.props.rowIndex));
    },
    _hasEmptyString: function (row) {
        for (var key in row) {
            if (row[key] == "") return true;
        }
        return false;
    },
    render: function () {
        var opts = utils.extends({}, this.props);
        delete opts.col;
        delete opts.onDoubleClick;

        var data = _getData(this.props.rowIndex, this.props.data, this.props.col, this.props);
        var cell = React.createElement(Cell, opts, data);

        var divOpts = {style: this._getStyle()};
        if (this._hasEmptyString(getData(this.props.data, this.props.rowIndex))) {
            divOpts.style.background = "#FFB5B5";
        }
        var cb = this.props.onDoubleClick;
        if (cb && typeof cb == "function") {
            divOpts.onDoubleClick = this._dblClick;
            divOpts.style.cursor = "pointer";
        }
        return React.createElement("div", divOpts, cell);
    }
});

var ComparisonTable = React.createClass({

    getDefaultProps: function () {
        return {
            version1: null,
            version2: null,
            content: null
        };
    },
    createContent: function (data) {
        var content = [];
        if (!data) return null;

        data.map(function (row) {
            var v1 = row["v1"];
            var v2 = row["v2"];
            var rowContent = [];
            if (!v1) {
                var tdList = [];
                for (var i = 0; i < 3; i++) {
                    tdList.push(<td></td>);
                }
                tdList.push(<td style={{borderRight:"#bbbbbb solid 1px"}}></td>);
                tdList.push(<td>{v2["columnName"]}</td>);
                tdList.push(<td>{v2["dataType"]}</td>);
                tdList.push(<td>{v2["dataLength"]}</td>);
                tdList.push(<td>{v2["dataScale"]}</td>);
                rowContent.push(<tr className="danger">{tdList}</tr>);
            } else if (!v2) {
                var tdList = [];
                tdList.push(<td>{v1["columnName"]}</td>);
                tdList.push(<td>{v1["dataType"]}</td>);
                tdList.push(<td>{v1["dataLength"]}</td>);
                tdList.push(<td style={{borderRight:"#bbbbbb solid 1px"}}>{v1["dataScale"]}</td>);
                for (var i = 0; i < 4; i++) {
                    tdList.push(<td></td>);
                }
                rowContent.push(<tr className="danger">{tdList}</tr>);
            } else {
                var v1TdList = [], v2TdList = [];
                const keyList=["columnName","dataType","dataLength","dataScale"];
                var isRowDifferent = false;
                keyList.map(function(key){
                    if(v1[key]!=v2[key]){
                        if(key==keyList[keyList.length-1]) {
                            v1TdList.push(<td style={{borderRight:"#bbbbbb solid 1px"}} className="danger">{v1[key]}</td>);
                        } else {
                            v1TdList.push(<td className="danger">{v1[key]}</td>);
                        }
                        v2TdList.push(<td className="danger">{v2[key]}</td>);
                        isRowDifferent=true;
                    } else {
                        if(key==keyList[keyList.length-1]) {
                            v1TdList.push(<td style={{borderRight:"#bbbbbb solid 1px"}}>{v1[key]}</td>);
                        } else {
                            v1TdList.push(<td>{v1[key]}</td>);
                        }

                        v2TdList.push(<td>{v2[key]}</td>);
                    }
                });
                if(isRowDifferent) {
                    rowContent.push(<tr className="warning">{v1TdList}{v2TdList}</tr>);
                } else {
                    rowContent.push(<tr>{v1TdList}{v2TdList}</tr>);
                }

            }
            content.push(rowContent);
        });
        return content;
    },
    render: function () {
        return (
            <div style={{overflowX:"auto",overflowY:"auto",height:"700px",width:"100%"}}>
            <table className="table">
                <thead>
                <tr className="h4">
                    <th colSpan="4">
                        {this.props.version1}
                    </th>
                    <th colSpan="4">
                        {this.props.version2}
                    </th>
                </tr>
                <tr className="h4">
                    <th>
                        Name
                    </th>
                    <th>
                        Type
                    </th>
                    <th>
                        Length
                    </th>
                    <th style={{borderRight:"#bbbbbb solid 1px"}}>
                        Scale
                    </th>
                    <th>
                        Name
                    </th>
                    <th>
                        Type
                    </th>
                    <th>
                        Length
                    </th>
                    <th>
                        Scale
                    </th>
                </tr>
                </thead>
                <tbody>
                {this.createContent(this.props.content)}
                </tbody>
            </table>
            </div>
        );
    }
});
module.exports = {
    BtnCell: BtnCell,
    TextCell: TextCell,
    ComparisonTable: ComparisonTable
};
