var express = require('express');
var router = express.Router();
var console = require('console');
var service = require('../lib/service/zk-service');
var ZooKeeper = require ('node-zookeeper-client');
var config = require('../config');
var client = ZooKeeper.createClient(config.zk.connect, { retries : 3 });
var Int64 = require('node-int64');
var logger = require('../lib/utils/logger');

router.get('/loadNodeAndDirectChildren', function (req, res) {
    var path = req.query["path"];
    var refreshSubTree = req.query["refreshSubTree"];
    if(!path) {
        res.json({status:404, message:"parameter 'path' not found"});
        return;
    }
    if(!refreshSubTree) {
        refreshSubTree=false;
    }
    let client = ZooKeeper.createClient(config.zk.connect,config.zk.client);
    var node={};
    client.once('connected', function () {
        client.getData(path, function (error, data, stat) {
            if (error) {
                console.log(error.stack);
                res.json({status: 500});
                return;
            }
            node.content=data+"";//Buffer 转换成字符串
            if(!refreshSubTree){
                res.json({data:node,status: 200});
                client.close();
            }else{
                service.loadSubTreeOfPath(path,function loadZkTreeOfPathCb(error,response) {
                    if(!error){
                        node.children=JSON.parse(response.body);
                    }
                    res.json({data:node,status: 200});
                    client.close();
                });
            }
        });
    });
    client.connect();
});

router.get('/loadZkTreeOfPath', function (req, res) {
    var path = req.query["path"];
    if(!path) {
        // logger.warn("parameter 'path' not found");
        res.json({status:404, message:"parameter 'path' not found"});
        return;
    }

    service.loadZkTreeOfPath(path,function loadZkTreeOfPathCb(error,response) {
        if(error){
            res.json({status: 500, data: '内部错误！'});
        }else{
            //console.log(response.body);
            res.json({status: 200, data: JSON.parse(response.body)});
        }
    });
});

router.get('/loadZkTreeByDsName', function (req, res) {
    var param = buildParam(req.query, ["dsName","dsType"]);
    if(!param["dsName"]){
        var msg="Parameter missing!";
        // logger.warn(msg);
        res.json({status:404, message:msg});
        return;
    }

    service.loadZkTreeByDsName(param["dsName"],param["dsType"],function loadZkTreeByDsNameCb(error,response) {
        if(error){
            res.json({status: 500, data: '内部错误！'});
        }else{
            res.json({status: 200, data: JSON.parse(response.body)});
        }
    });
});

router.get('/cloneConfFromTemplate', function (req, res) {
    var self=this;
    var param = buildParam(req.query, ["dsName","dsType"]);
    if(!param["dsType"]||!param["dsName"]){
        var msg="Parameter missing!";
        // logger.warn(msg);
        res.json({status:404, message:msg});
        return;
    }
    //service.cloneConfFromTemplate(param["dsType"],param["dsName"],function cloneConfFromTemplateCb(error,response) {
    service.cloneConfFromTemplate(param,function cloneConfFromTemplateCb(error,response) {
        if(error){
            res.json({status: 500, data: '内部错误！'});
        }else{
            //res.json({status: 200, data: JSON.parse(response.body)});
            res.json({status: 200});
        }
    });
});

router.post('/modifyZkNodeData', function (req, res) {
    var param = {};
    //param["path"] = req.query["path"];
    //param["data"] = req.query["data"];
    param["path"] = req.param("path");
    param["data"] = req.param("data");
    //var param = buildParam(req.query,["path","data"]);
    //if(!param["path"]||!param["data"]){
    if(!param["path"]||(param["data"] == null)){
        var msg="Parameter missing!";
        res.json({status:404, message:msg});
        return;
    }
    let path = param["path"];
    path = path.replace(/=2/g, ':');
    let client = ZooKeeper.createClient(config.zk.connect,config.zk.client);
    client.once('connected', function () {
        console.log('Connected to the server.');
        var data=new Buffer(param["data"]);
        client.setData(path, data, function (error, stat) {
            if (error) {
                console.log(error.stack);
                res.json({status: 500});
                return;
            }
            res.json({status: 200});
            client.close();
        });
    });
    client.connect();
});

router.post('/addZkNodeOfPath', function(req, res){
    var path = req.param("path");
    var nodeName = req.param("nodeName");
    var newNodePath = "";
    if(path === "/"){
        newNodePath = path.concat(nodeName);
    }else{
        newNodePath = path.concat("/").concat(nodeName);
    }
    //const newNodePath = path.concat('/').concat(nodeName);
    console.log(newNodePath);
    if(!path) {
        res.json({status:404, message:"parameter 'path' not found"});
        return;
    }
    let client = ZooKeeper.createClient(config.zk.connect,config.zk.client);
    client.once('connected', function () {
        client.create(newNodePath, null, function (error, path) {
            if (error) {
                console.log(error.stack);
                res.json({status: 500});
                return;
            }
            res.json({status: 200,newNodePath:path});
            client.close();
        });
    });
    client.connect();
});
/*
function deleteZkNode(client, path) {
    client.getChildren(path, function (error, children, stat) {
        if (error) {
            console.log('Failed to list children of %s due to: %s.', path, error);
            return;
        }
        console.log('Children are: %j.', children);
        console.log(children.length);
        if (children.length > 0) {
            children.map((child) => {
                let childPath = path.concat('/').concat(child);
                console.log(childPath);
                deleteZkNode(client, childPath);
            });
        }else{
            console.log("will delete node %s ", path);
            client.remove(path, -1, function (error) {
                if (error) {
                    console.log(error.stack);
                    res.json({status: 500});
                    return {status: 500};
                }
                console.log("delete zk node %s succeeded", path);
                return {status: 200}
            });
        }

    });
};
*/
router.get('/deleteZkNodeOfPath', function(req, res){
    var path = req.param("path");
    if(!path) {
        res.json({status: 404, message: "parameter 'path' not found"});
        return;
    }
    path = path.replace(/=2/g, ':');
    service.deleteZkNodeOfPath(path,function deleteZkNodeOfPathCb(error,response) {
        if(error){
            res.json({status: 500, data: '内部错误！'});
        }else{
            res.json({status: 200});
        }
    });
});

router.post('/refreshZkNodeOfPath', function(req, res){
    var path = req.param("path");
    if(!path) {
        res.json({status:404, message:"parameter 'path' not found"});
        return;
    }
    var node={};
    service.loadSubTreeOfPath(path,function loadZkTreeOfPathCb(error,response) {
        if(!error){
            node.children=JSON.parse(response.body);
        }
        console.log(node);
        res.json({data:node,status: 200});
    });

});
/*
router.get('/getZkNodeOfPath', function (req, res) {
    var path = req.query["path"];
    if(!path) {
        res.json({status:404, message:"parameter 'path' not found"});
        return;
    }
    console.log(path);
    client.once('connected', function () {
        client.getACL(path, function (error, acls, stat) {
            if (error) {
                console.log(error.stack);
                return;
            }
            var aclsInfo = eval(acls);
            node.permission = aclsInfo[0].permission;
            node.scheme = aclsInfo[0].id.scheme.toString();
            node.id = aclsInfo[0].id.id.toString();
            res.json({status: 200,data: node});

        });
    });
    client.connect();
    var node={};
    service.getZkNodeOfPath(path,function getZkNodeOfPathCb(error,response) {
        if(!error){
            node.children=JSON.parse(response.body);
        }
        console.log(node);
        res.json({data:node,status: 200});
    });

});
*/

router.get('/getZkNodeOfPath', function (req, res) {
    var path = req.query["path"];
    if(!path) {
        res.json({status:404, message:"parameter 'path' not found"});
        return;
    }
    path = path.replace(/=2/g, ':');
    var node={};
    try{
        let client = ZooKeeper.createClient(config.zk.connect,config.zk.client);
        client.once('connected', function () {
            client.getACL(path, function (error, acls, stat) {
                if (error) {
                    //console.log(error.stack);
                    logger.info(error.stack);
                    res.json({status:500, message:"get acl error"});
                    return;
                }
                //node Acl
                var aclsInfo = eval(acls);
                logger.info(aclsInfo.length);
                if(aclsInfo.length > 0){
                    node.permission = aclsInfo[0].permission;
                    node.scheme = aclsInfo[0].id.scheme.toString();
                    node.id = aclsInfo[0].id.id.toString();
                }
                //node meta data
                node.czxid = new Int64(stat.czxid).toNumber(true);
                node.mzxid = new Int64(stat.mzxid).toNumber(true);
                node.ctime = new Int64(stat.ctime).toNumber(true);
                node.mtime = new Int64(stat.mtime).toNumber(true);
                node.version = stat.version;
                node.cversion = stat.cversion;
                node.aversion = stat.aversion;
                node.ephemeralOwner = new Int64(stat.ephemeralOwner).toNumber(true);
                node.dataLength = stat.dataLength;
                node.numChildren = stat.numChildren;
                node.pzxid = new Int64(stat.pzxid).toNumber(true);
                if(node.scheme !== "world"){
                    node.content = "";
                    logger.info("Authontication is node world.");
                    res.json({status: 200,data: node});
                    return;
                }
                client.getData(path, function (error, data, stat) {
                    if (error) {
                        //console.log(error.stack);
                        logger.info(error.stack);
                        res.json({status: 500, data: '内部错误！'});
                        return;
                    }
                    if(typeof data === "undefined" || data === null){
                        node.content = "";
                        res.json( { status: 200, data: node } );
                    }else{
                        node.content= data + "";
                        res.json( { status: 200, data: node } );
                    }
                    client.close();
                });
            });
        });
        client.connect();
    }catch (error){
        logger.info(error);
    }

});

router.get('/loadLevelOfPath', function (req, res) {
    var path = req.query["path"];
    if(!path) {
        // logger.warn("parameter 'path' not found");
        res.json({status:404, message:"parameter 'path' not found"});
        return;
    }
    service.loadLevelOfPath(path,function loadZkTreeOfPathCb(error,response) {
        if(error){
            res.json({status: 500, data: '内部错误！'});
        }else{
            res.json({status: 200, data: JSON.parse(response.body)});
        }
    });
});

router.get('/searchNodeOfValue',function (req, res) {
    var searchValue = req.query["searchValue"];
    var selectPath = req.query["selectPath"];
    if(!searchValue || !selectPath){
        res.json({status:404, message:"parameter 'searchValue' or 'selectPath' not found"});
        return;
    }
    console.log("zk.js test");
    service.searchNodeOfValue(searchValue, selectPath, function searchNodeOfValueCb(error, response){
        if(error){
            res.json({status: 500, data: '内部错误！'});
        }else{
            res.json({status: 200, data: JSON.parse(response.body)});
        }
    });
});
// -----------------------------------------------------------------
// todo   apply to related business codes
router.get('/applyTemplateConfToBusiness', function (req, res) {
    var param = buildParam(req.query,["templatePath","dsNames"]);
    if(!param["templatePaths"]||!param["dsNames"]){
        var msg="Parameter missing!";
        // logger.warn(msg);
        res.json({status:404, message:msg});
        return;
    }

    var pathsArr=param["templatePaths"].split(";");
    pathsArr.forEach(function (templateNodePath) {
        var templateNodePath=businessNodePath.replace(TEMPLATE_ROOT,BUSSINESS_ROOT);
        //js replace 用正则表达式才可以替换全部。正则里面不能用变量名，必须直接用值。所以没再引用定义的常量，而是直接写死了
        businessNodePath=businessNodePath.replace(/placeholder/g,dsName);
        businessNodePath=businessNodePath.replace(/placeholderType/g,dsType);

    });
    let client = ZooKeeper.createClient(config.zk.connect,config.zk.client);
    client.once('connected', function () {
        console.log('Connected to the server.');
        var data=new Buffer(param["data"]);
        client.setData(param["path"], data, function (error, stat) {
            if (error) {
                console.log(error.stack);
                res.json({status: 500});
                return;
            }
            res.json({status: 200});
            client.close();
        });
    });
    client.connect();
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

module.exports = router;
