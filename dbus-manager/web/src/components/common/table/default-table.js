var React = require('react');
var ReactDOM = require('react-dom');
var Tab = require('fixed-data-table');
var debounce = require('lodash/debounce');
var utils = require('../utils');
var $ = require('jquery');
//var assign = require('lodash/assign');
//var isEqual = require('lodash/isEqual');

var Table = Tab.Table;

var initialPixels = 0;

var getDefaultValue = function(val, defVal) {
    return val ? val : defVal;
};

var DefaultTable = React.createClass({
    __isMounted: false,
    getInitialState: function() {
        return {
            width: initialPixels,
            scrollBarHeight:0
        };
    },
    getDefaultProps: function() {
        return {
            refreshRate: 10
        };
    },
    componentDidMount: function() {
        this.__isMounted = true;
        this._setDimensionsOnState();
        this._attachResizeEvent();
    },
    shouldComponentUpdate: function(nextProps, nextState) {
        return true;
        //return !isEqual(this.props, nextProps) || !isEqual(this.state, nextState);
    },
    componentWillMount: function() {
        var refreshRate = this.props.refreshRate;
        this._setDimensionsOnState = debounce(this._setDimensionsOnState, refreshRate);
    },
    componentWillUnmount: function() {
        this.__isMounted = false;
        window.removeEventListener('resize', this._setDimensionsOnState);
    },
    _attachResizeEvent: function() {
        if (window.addEventListener) {
            window.addEventListener('resize', this._setDimensionsOnState, false);
        } else if (window.attachEvent) {
            window.attachEvent('resize', this._setDimensionsOnState);
        } else {
            window.onresize = this._setDimensionsOnState;
        }
    },
    _setDimensionsOnState: function() {
        if (this.__isMounted) {
            var offsetWidth = ReactDOM.findDOMNode(this).offsetWidth;
            this.setState({
                width: offsetWidth || initialPixels,
                scrollBarHeight: 0
            });
            this._extendTable();
        }
    },
    _extendTable: function() {
        var scrollBar = $(ReactDOM.findDOMNode(this)).find(".fixedDataTableLayout_horizontalScrollbar");
        var h = 0;
        if(scrollBar.length > 0) {
            h = scrollBar.height();
        }
        var state = this.state;
        state.scrollBarHeight = h;
        this.setState(state);
    },
    _getStyle: function() {
        return {
            width: '100%'
        };
    },
    _createTable: function(props) {
        var defaultOpts = {
            ref: "table",
            rowHeight: getDefaultValue(props.rowHeight, 36),
            rowsCount: props.rowsCount,
            width: getDefaultValue(this.state.width, 1000),
            height: getDefaultValue(props.height, getDefaultValue(props.rowHeight, 36) * props.rowsCount + getDefaultValue(props.headerHeight, 36) + this.state.scrollBarHeight),
            headerHeight: getDefaultValue(props.headerHeight, 36),
            overflowX: "auto",
            overflowY: "hidden"
        };
        if(defaultOpts.height <= 100) {
            defaultOpts.height = 100;
        }
        // defaultOpts.height = defaultOpts.rowsCount * defaultOpts.rowHeight + defaultOpts.headerHeight + this.state.scrollBarHeight;
        var options = utils.extends({}, defaultOpts);
        options = utils.extends(options, props);
        delete options["children"];
        var table = React.createElement(Table, options, props.children);
        return table;
    },
    render: function() {
        return (
            <div style={this._getStyle()}>
                {this._createTable(this.props)}
            </div>
        );
    }
});


module.exports = DefaultTable;
