var React = require('react');

/**
 * 顶部导航栏搜索框组件
 */
var Searcher = React.createClass({
    render: function (){
        return (
            <li className="hidden-xs">
                <input className="search" type="text" />
            </li>
        );
    }
});

module.exports = Searcher;
