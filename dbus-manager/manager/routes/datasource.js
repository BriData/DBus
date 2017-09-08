var express = require('express');
var router = express.Router();
var console = require('console');
var Utils = require('../lib/utils/utils');
var ds = require('../lib/service/ds-service');

router.get('/list', function (req, res) {
    ds.list(function loadDsList(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        var resultList = JSON.parse(response.body);
        var dsList = [];
        for(var i = 0; i < resultList.length; i++) {
            var ds = {};
            Utils.extend(ds, resultList[i], ["id", "dsName", "dsType","masterURL","slaveURL","dbusUser","dbusPassword"]);
            dsList.push(ds);
        }
        res.json({status: 200, data: dsList});
    });
});

router.get('/search', function (req, res) {
    var param = buildParam(req.query, [ "text", "pageSize", "pageNum"]);
    if (!param.pageSize||param.pageSize != 20) {
        param.pageSize = 20;
    }
    if (!param.pageNum) {
        param.pageNum = 1;
    }
    ds.search(param, function searchDs(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200, data: response.body});
    });
});


router.get('/searchFromSource', function (req, res) {
    var param = buildParam(req.query, [ "dsId" ,"dsType","URL","user","password"]);
     ds.getPassword(param.dsId,function(err,response) {
        param.password = JSON.parse(response.body).dbusPassword;


      ds.searchFromSource(param, function searchFromSource(err, response) {
          if(err) {
              res.json({status: 500, message: err.message});
              return;
          }
          var resultList = response.body;
          var structureList = [];
          var string ="";//保存相同表名的列结构字符串之和
          var flag ="";
          var pro = false;//区分最后一次表的添加与过程的添加
          for(var i = 0;i < resultList.length; i++){
            var s = [];
            s = resultList[i].toString().split("/");
            if(i == 0){
                flag = s[0];//标兵，标示连续相同的表名
            }
            if(flag == s[0]){
                string += " "+s[1];
            }else{
                //检查存储过程
                if(s[1] == "--------"){
                    if(pro == false) {
                        structureList.push({type: "表", name: flag, exist: "是", column: string});//添加最后一个表
                        pro = true;
                    }else{
                        structureList.push({type: "存储过程", name: flag, exist: "是", column: string});
                    }
                }else {
                    structureList.push({type: "表", name: flag, exist: "是", column: string});
                }
                flag =s[0];
                string = s[1];
            }
          }
          if(param["dsType"] == "oracle" && resultList.status != -1) {
              structureList.push({type: "存储过程", name: flag, exist: "是", column: string});
          }else if(param["dsType"] == "mysql" && resultList.status != -1){
              structureList.push({type: "表", name: flag, exist: "是", column: string});
          }
          res.json({status: 200, data: structureList});
      });

     });

});


router.get('/add', function (req, res) {
    var param = buildParam(req.query, [ "dsName", "dsType", "status","dsDesc","topic","ctrlTopic","schemaTopic","splitTopic","masterURL","slaveURL","dbusUser","dbusPassword"]);
    ds.add(param,function addDs(err,response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200,id:response.body});
    });
});

router.get('/active', function (req, res) {
    var param = buildParam(req.query, [ "id", "status"]);
    ds.active(param,function activeDs(err,response) {
        console.log(response.body);
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200, data: response.body});
    });
});

router.get('/inactive', function (req, res) {
    var param = buildParam(req.query, [ "id", "status"]);
    ds.inactive(param,function inactiveDs(err,response) {
        console.log(response.body);
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200, data: response.body});
    });
});

router.get('/validate',function(req,res){
    var masterParam = buildValidateParam(req.query,["dsType","masterURL","user","password"]);
    var slaveParam = buildValidateParam(req.query,["dsType","slaveURL","user","password"]);
    var masterData = 0;
    var slaveData = 0;
    ds.validate(masterParam,function validateDs(err, response) {
        console.log(response.body);
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
        //res.json({status: 200, data: response.body});
        masterData =  response.body;
        ds.validate(slaveParam,function validateDs(err, response) {
                    console.log(response.body);
                    if(err) {
                        res.json({status: 500, message: err.message});
                        return;
                    }
                    //res.json({status: 200, data: response.body});
                    slaveData =  response.body;
                    if(masterData == 1 && slaveData == 1){
                      res.json({status: 200, data: 1});
                    }else{
                        res.json({status: 200, data: -1});
                    }
                }
        );
    }
    );

});

router.get('/updateDs',function(req,res){
    var param = buildParam(req.query,["id","dsDesc","masterURL","slaveURL"]);
    ds.updateDs(param,function updateDs(err, response) {
            console.log(response.body);
            if(err) {
                res.json({status: 500, message: err.message});
                return;
            }
            res.json({status: 200, data: response.body});
        }
    );
});

router.get('/checkName', function (req, res) {
    var param = buildParam(req.query, [ "dsName"]);
    ds.check(param, function checkDsName(err, response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200, data: response.body});
    });
});

var buildParam = function (query, params) {
    var param = {};
    params.forEach(function (key) {
        if (query[key]) {
            param[key] = query[key];
        }
    });
    return param;
}

var buildValidateParam = function (query, params) {
    var param = {};
    params.forEach(function (key) {
        if (query[key]) {
            if(key == "masterURL" || key == "slaveURL"){
                param["URL"] = query[key];
            }else {
                param[key] = query[key];
            }
        }
    });
    return param;
}
module.exports = router;
