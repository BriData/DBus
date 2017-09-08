var React = require('react');
var Navbar = require('./common/navbar/navbar');
var Sidebar = require('./common/sidebar/sidebar');
var Content = require('./content');
var Loading = require('./common/loading');
var App = React.createClass({
    render: function() {
        return (
            <div className="App">
                <Loading />
                <Navbar />
                <Sidebar />
                <Content>
                    { this.props.children }
                </Content>
            </div>
        );
    }
});

module.exports = App;
