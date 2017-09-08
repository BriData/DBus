/**
 * 生成4位验证码
 * Created by zhangyf on 16/9/20.
 */
var express = require('express');
var router = express.Router();
var console = require('console');
var ccap = require('ccap');

var seed = ['2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'];

var generate = function () {
    var str_num = 4,
        r_num = seed.length,
        text = '';
    for (var i = 0; i < str_num; i++) {
        var pos = Math.floor(Math.random() * r_num)
        text += seed[pos];//生成随机数
    }
    return text;
};

var captcha = ccap({
    width: 135,
    height: 40,
    offset: 30,
    quality: 100,
    fontsize: 40,
    generate: function () {
        return generate();
    }
});

router.get('/', function (req, res) {
    var ary = captcha.get();
    var txt = ary[0];
    var buf = ary[1];
    console.log(txt);
    res.write(buf);
    res.end();
});

module.exports = router;