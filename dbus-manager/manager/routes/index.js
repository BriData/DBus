var express = require('express');
var router = express.Router();
var filter = require('../lib/filter');

router.get('/login', function (req, res) {
    var userName = req.param("userName");
    var pwd = req.param("pwd");
    filter.authorize(userName, pwd, function (err, user) {
        if(err) {
            res.json(err);
            return;
        }
        req.session.regenerate(function () {
            req.session.user = user;
            req.session.save();
            res.json({status: 200, message: 'Ok'});
        });
    });
});

router.get('/logout', function (req, res) {
    if('/logout' == req.url){
        req.session.user = null;
        res.json({status: 200, message: 'Ok'});
    }
});


module.exports = router;
