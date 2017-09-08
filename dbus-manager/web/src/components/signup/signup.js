var React = require('react');
var ReactDOM = require('react-dom');
var $ =require('jquery');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var store = require('./signup-store');
var Link = require('react-router').Link;
var utils = require('../common/utils');


var Signup = React.createClass({
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
    signup:function(){
        var account = this.ref.account.value;
        var password = this.ref.password.value;
        //执行注册动作，如果注册成功，则跳转到登录页面。
        this.props.history.pushState({}, "/login"); 
    },
    render: function() {
        return (
        <div className="login login-bg">
            <div className="row-fluid login-wrapper">
                <div className="col-md-4 box">
                    <div className="content-wrap">
                        <h6>Sign up</h6>
                        <input ref="account" className="form-control" type="text" placeholder="Your account" />
                        <input ref="password" className="form-control" type="password" placeholder="Your password" />
                        <B.Button
                        className="btn btn-defaultg"
                        onClick={this.signup}>
                        sign up
                        </B.Button>
                    </div>
                </div>
            </div>
        </div>
        );
    }
});




module.exports = Signup;