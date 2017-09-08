var React = require('react');
var Link = require('react-router').Link;

/**
 * 侧边栏导航项
 */
var SidebarItem = React.createClass({

    /** component id */
    id : 0,

    /** 菜单项click事件的处理方法 */
    onClick: function(){
        this.props.callback(this.id);
    },

    setId: function(id) {
        this.id = id;
    },

    render: function(){
        var cssName = this.props.item.isActive ? "active" : "inactive";
        this.setId(this.props.item.id);
        return (
            <li className={cssName} onClick={ this.onClick }>
                <div className="pointer">
                    <div className="arrow"></div>
                    <div className="arrow_border"></div>
                </div>
                <Link to={this.props.item.href}>
                    <span className={this.props.item.icon}></span>
                    <span>{this.props.item.text}</span>
                </Link>
            </li>
        );
    }
});

module.exports = SidebarItem;
