var React = require('react');
var ReactDOM = require('react-dom');
var $ =require('jquery');
var Reflux = require('reflux');
var B = require('react-bootstrap');
var store = require('./login-store');
var Link = require('react-router').Link;
var utils = require('../common/utils');


var Login = React.createClass({
    mixins: [Reflux.listenTo(store, "_onStatusChange")],
    getInitialState: function() {
        var state = store.initState();
        return state;
    },
    componentDidMount: function() {
        //store.actions.initialLoad();
        var self = this;
        var doLogin = function(e) {
            if(e.keyCode == 13) {
                self.login();
            }
        };
        $(this.refs.user).keydown(doLogin);
        $(this.refs.password).keydown(doLogin);
    },
    _onStatusChange: function(state) {
        this.setState(state);
    },
    login:function(){
        var user = this.refs.user.value;
        var password = this.refs.password.value;
        store.actions.login(user,password,this.redirect);
    },
    redirect:function(flag){
        if(flag == 1){
           this.props.history.pushState({}, "/");
       }
    },
    render: function() {
        return (
        <div className="login login-bg">
            <div className="row-fluid login-wrapper">
                <div className="col-md-4 box">
                    <div className="content-wrap">
                        <h6>Log in</h6>
                        <input ref="user" className="form-control" type="text" placeholder="Your account" />
                        <input ref="password" className="form-control" type="password" placeholder="Your password" />
                        <div className="remember">
                            <input id="remember-me" type="checkbox" />
                            <label for="remember-me">Remember me</label>
                        </div>
                        <B.Button
                        className="btn btn-defaultg"
                        onClick={this.login}>
                        Log in
                        </B.Button>
                    </div>
                </div>
            </div>
        </div>
        );
    }
});




module.exports = Login;
