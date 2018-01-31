/**
 * Created by haowei6 on 2017/11/20.
 */
var React = require('react');
var ReactDOM = require('react-dom');
var utils = require('../common/utils');
var B = require('react-bootstrap');

var CustomCompareTable = React.createClass({

    acreateContent: function (data) {
        var content = [];
        if (!data) return null;

        data.map(function (row) {
            var v1 = row["v1"];
            var v2 = row["v2"];
            var rowContent = [];
            if (!v1) {
                var tdList = [];
                for (var i = 0; i < len - 1; i++) {
                    tdList.push(<td></td>);
                }
                tdList.push(<td style={{borderRight:"#bbbbbb solid 1px"}}></td>);
                for (var i = 0; i < len; i++) {
                    tdList.push(<td>{v2[keyList[i]]}</td>);
                }
                rowContent.push(<tr className="danger">{tdList}</tr>);
            } else if (!v2) {
                var tdList = [];
                for (var i = 0; i < len - 1; i++) {
                    tdList.push(<td>{v1[keyList[i]]}</td>);
                }
                tdList.push(<td style={{borderRight:"#bbbbbb solid 1px"}}>{v1[keyList[len - 1]]}</td>);
                for (var i = 0; i < len; i++) {
                    tdList.push(<td></td>);
                }
                rowContent.push(<tr className="danger">{tdList}</tr>);
            } else {
                var v1TdList = [], v2TdList = [];
                var isRowDifferent = false;
                keyList.map(function(key){
                    if(v1[key]!=v2[key]){
                        if(key==keyList[len-1]) {
                            v1TdList.push(<td style={{borderRight:"#bbbbbb solid 1px"}} className="danger">{v1[key]}</td>);
                        } else {
                            v1TdList.push(<td className="danger">{v1[key]}</td>);
                        }
                        v2TdList.push(<td className="danger">{v2[key]}</td>);
                        isRowDifferent=true;
                    } else {
                        if(key==keyList[len-1]) {
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
    createTitle: function () {
        var title = this.props.title;
        if (title.length == 0) return "Nothing to diff, please choose group";
        var subTitle = this.props.subTitle;
        var len = subTitle.length;
        return title.map(function (item) {
            return (<th colSpan={len}>
                {item}
            </th>)
        });
    },
    createSubTitle: function () {
        var title = this.props.title;
        var subTitle = this.props.subTitle;
        return title.map(function (titleItem, titleIndex) {
            return subTitle.map(function (item, index) {
                if (index == subTitle.length - 1 && titleIndex != title.length - 1) {
                    return (<th style={{borderRight:"#bbbbbb solid 1px"}}>{item}</th>)
                }
                return (<th>{item}</th>)
            })
        })
    },
    createContent: function () {
        var subTitle = this.props.subTitle;
        var content = this.props.content;

        var maxRow = ((content) => {
            var maxRow = 0;
            content.forEach(function (item) {
                if (item.length > maxRow) maxRow = item.length;
            });
            return maxRow;
        })(content);

        var checkRowDiff = (row) => {
            for (var i = 0; i < content.length - 1; i++) {
                var left = content[i][row];
                var right = content[i + 1][row];
                if (left == null && right == null) continue;
                if (left == null || right == null) return true;
                for (var j = 0; j < subTitle.length; j++) {
                    if (left[j] != right[j]) return true;
                }
            }
            return false;
        };

        var drawTdInTr = function(row) {
            var ret = [];
            for (var part = 0; part < content.length; part++) {
                var current = content[part][row];
                for (var col = 0; col < subTitle.length; col++) {
                    var tdContent = <div>&nbsp;</div>;
                    if (current != null) {
                        tdContent = current[col];
                    }
                    var tag = (<td>{tdContent}</td>);
                    if (col == subTitle.length - 1 && part != content.length - 1) {
                        tag = (<td style={{borderRight:"#bbbbbb solid 1px"}}>{tdContent}</td>);
                    }
                    ret.push(tag);
                }
            }
            return ret;
        };

        var diffRow = function (row) {
            return (<tr className="danger">{drawTdInTr(row)}</tr>);
        };
        var sameRow = function (row) {
            return (<tr>{drawTdInTr(row)}</tr>);
        };
        var ret = [];
        for (var row = 0; row < maxRow; row++) {
            if(checkRowDiff(row)) {
                ret.push(diffRow(row));
            } else {
                ret.push(sameRow(row));
            }
        }
        return ret;
    },
    render: function () {
        return (
            <div style={{tableLayout:"fixed",overflowX:"auto",overflowY:"auto",height:"720px",width:"100%"}}>
                <table className="table">
                    <thead>
                        <tr className="h4">
                            {this.createTitle()}
                        </tr>
                        <tr className="h4">
                            {this.createSubTitle()}
                        </tr>
                    </thead>
                    <tbody>
                        {this.createContent()}
                    </tbody>
                </table>
            </div>
        );
    }
});

module.exports = CustomCompareTable;
