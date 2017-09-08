var React = require('react')
var debounce = require('lodash/debounce');

var Content = React.createClass({
    __isMounted: false,
    __CORRECTION: 52,
    __clientHeight: function() {
        return window.innerHeight || document.documentElement.clientHeight || document.body.clientHeight;
    },
    getInitialState: function() {
        return {
            height: this.__clientHeight() - this.__CORRECTION
        };
    },
    getDefaultProps: function() {
        return {refreshRate: 10};
    },
    componentDidMount: function() {
        this.__isMounted = true;
        this._changeHeight();
        this._attachResizeEvent();
    },
    shouldComponentUpdate: function(nextProps, nextState) {
        return true;
    },
    componentWillMount: function() {
        var refreshRate = this.props.refreshRate;
        this._changeHeight = debounce(this._changeHeight, refreshRate);
    },
    componentWillUnmount: function() {
        this.__isMounted = false;
        window.removeEventListener('resize', this._changeHeight);
    },
    _attachResizeEvent: function() {
        if (window.addEventListener) {
            window.addEventListener('resize', this._changeHeight, false);
        } else if (window.attachEvent) {
            window.attachEvent('resize', this._changeHeight);
        } else {
            window.onresize = this._changeHeight;
        }
    },
    _changeHeight: function() {
        if (this.__isMounted) {
            this.setState({
                height: this.__clientHeight() - this.__CORRECTION
            });
        }
    },
    _getStyle: function() {
        return {minHeight: this.state.height};
    },
    render: function() {
        return (
            <div className='content' style={this._getStyle()}>
                {this.props.children}
            </div>
        )
    }
})

module.exports = Content
