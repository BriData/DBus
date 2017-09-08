var React = require('react');
var B = require('react-bootstrap');
var Searcher = require('./navbar-searcher');
var store = require('./navbar-store');
var utils = require('../utils');
var $ = require('jquery')

var ReactDOM = require('react-dom');
var Reflux = require('reflux');
var Link = require('react-router').Link;

var NavBar = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    componentDidMount: function() {
        store.actions.initialLoad();
    },
    _onStatusChange: function(state) {
        this.setState(state);
    },
    logout: function () {
        store.actions.logout(this.redirect);
    },
    redirect: function (flag) {
        $.get(utils.builPath("logout"), function() {
            document.location.href = "/#/login"
        })
    },

    render: function() {
        return (
            <B.Navbar inverse>
                <B.Navbar.Header>
                    <B.Navbar.Brand>
                        <a href="#">DBus</a>
                    </B.Navbar.Brand>
                    <B.Navbar.Toggle />
                </B.Navbar.Header>
                <B.Navbar.Collapse>
                    <B.Nav pullRight>
                        <B.NavItem href="mailto:bridata@126.com"><span className="glyphicon glyphicon-envelope"></span></B.NavItem>
                        <B.NavItem href="#config"><span className="glyphicon glyphicon-cog"></span></B.NavItem>
                        <B.NavDropdown title="My Account" id="basic-nav-dropdown">
                            <B.MenuItem onClick={this.logout}><span className="glyphicon glyphicon-log-out"></span> logout</B.MenuItem>
                        </B.NavDropdown>
                    </B.Nav>
                </B.Navbar.Collapse>
            </B.Navbar>
        );
    }
});

module.exports = NavBar;
