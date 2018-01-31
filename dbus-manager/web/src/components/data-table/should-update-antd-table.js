/**
 * Created by haowei6 on 2017/11/29.
 */
var React = require('react');
var ReactDOM = require('react-dom');
var antd = require('antd');
var $ = require('jquery');
var utils = require('../common/utils');
var ShouldUpdateAntdTable = React.createClass({
    shouldComponentUpdate: function (nextProps, nextState) {
        if (!utils.objectDeepEqual(nextProps.resultSource, this.props.resultSource, 3)
            || !utils.objectDeepEqual(nextProps.resultColumns, this.props.resultColumns, 3)
            ) return true;
        return false;
    },
    componentDidMount() {
        var antdTable = ReactDOM.findDOMNode(this.refs.antdTable);
        for (var i = 0; i < 6; i++) antdTable = antdTable.childNodes[0];
        var self = this;
        antdTable.onscroll = function () {
            var antdSlider = ReactDOM.findDOMNode(self.refs.antdSlider);
            var track = antdSlider.childNodes[1];
            var Role = antdSlider.childNodes[3];
            var max = antdTable.scrollWidth - antdTable.clientWidth;
            track.style.width = (antdTable.scrollLeft / max * 100) + '%';
            Role.style.left = (antdTable.scrollLeft / max * 100) + '%';
        };
    },
    onSliderChange(value) {
        var antdTable = ReactDOM.findDOMNode(this.refs.antdTable);
        for (var i = 0; i < 6; i++) antdTable = antdTable.childNodes[0];
        var max = antdTable.scrollWidth - antdTable.clientWidth;
        antdTable.scrollLeft = value / 10000 * max;
    },
    render: function () {
        return (
            <div>
                <div className="col-xs-12">
                    <antd.Affix>
                        <antd.Slider ref="antdSlider" max={10000} onChange={this.onSliderChange} tipFormatter={null}/>
                    </antd.Affix>
                </div>
                <div className="col-xs-12">
                    <antd.Table ref="antdTable" rowClassName={() => {return "ant-table-row-custom-td"}}
                                locale={{emptyText:"No log"}}
                                pagination={false}
                                scroll={{x:true}}
                                bordered
                                size="default"
                                dataSource={this.props.resultSource}
                                columns={this.props.resultColumns}>
                    </antd.Table>
                </div>
            </div>
        );
    }
});

module.exports = ShouldUpdateAntdTable;
