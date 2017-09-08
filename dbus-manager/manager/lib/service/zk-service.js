var $ = require("../utils/utils");
var config = require('../../config');
var request = require('request');
var ZooKeeper = require ('node-zookeeper-client');
var client = ZooKeeper.createClient(config.zk.connect);
/*
var TEMPLATE_NODE_NAME='/ConfTemplates';
var TEMPLATE_ROOT_TOPOLOGY='/DBus/ConfTemplates/Topology';
var TEMPLATE_ROOT_EXTRACTOR='/DBus/ConfTemplates/Extractor';

var BUSSINESS_PLACEHOLDER='placeholder'; //js replace 用正则表达式才可以替换全部。正则里面不能用变量名，必须直接用值。没再引用这个常量，而是直接写死了
*/
var restUrl = config.rest.dbusRest;
module.exports = {
    loadZkTreeOfPath: function (path, cb) {
        var url = $.url(restUrl, "/zookeeper/loadZkTreeOfPath?path="+path);
        request.get({url: url, forever: true}, $.resWrapper(cb));
    },
    loadSubTreeOfPath: function (path, cb) {
        var url = $.url(restUrl, "/zookeeper/loadSubTreeOfPath?path="+path);
        request.get({url: url, forever: true}, $.resWrapper(cb));
    },
    loadZkTreeByDsName: function (dsName, cb) {
        var url = $.url(restUrl, "/zookeeper/loadZkTreeByDsName/"+dsName);
        request.get({url: url, forever: true}, $.resWrapper(cb));
    },
    cloneConfFromTemplate:function(param, cb){
        var url = $.url(restUrl, "/zookeeper/cloneConfFromTemplate");
        request.get({url: url, json: param, forever: true}, $.resWrapper(cb));
    },
/*
    cloneConfFromTemplate:function(dsType, dsName, cb){
        var self=this;
        client.once('connected', function () {
            self.cloneNodeRecursively(TEMPLATE_ROOT_TOPOLOGY, dsName);
            if(dsType.toLowerCase()=="mysql"){
                self.cloneNodeRecursively(TEMPLATE_ROOT_EXTRACTOR, dsName);
            }
            cb();// todo ??????????
        });

        client.connect();
    },

    cloneNodeRecursively:function(zkNode, dsName){
        var self=this;
        client.getChildren(zkNode, function (error, children, stats) {
            if (error) {
                console.log(error.stack);
                return;
            }
            var childrenCount=children.length;
            if(childrenCount>0){
                children.forEach(function (node) {
                    var templateNodePath=zkNode+"/"+node;
                    self.cloneNodeRecursively(templateNodePath,dsName);
                });
            }else{
                self.populateLeafNodeData(zkNode,dsName);
            }
        });
    },

    populateLeafNodeData:function(templateNodePath,dsName){
        var businessNodePath=templateNodePath.replace(TEMPLATE_NODE_NAME,"");
        //js replace 用正则表达式才可以替换全部。正则里面不能用变量名，必须直接用值。所以没再引用定义的常量，而是直接写死了
        businessNodePath=businessNodePath.replace(/placeholder/g,dsName);

        client.getData(
            templateNodePath,
            function (event) {
                console.log('Got event: %s.', event);
            },
            function (error, data, stat) {
                if (error) {
                    console.log(error.stack);
                    return;
                }

                client.mkdirp(businessNodePath, function (error, businessNodePath) {
                    if (error) {
                        console.log(error.stack);
                        return;
                    }
                    if(data){
                        data=data.toString();
                        //js replace 用正则表达式才可以替换全部。正则里面不能用变量名，必须直接用值。所以没再引用定义的常量，而是直接写死了
                        data=data.replace(/placeholder/g,dsName);
                        client.setData(businessNodePath, new Buffer(data), function (error, stat) {
                            if (error) {
                                console.log(error.stack);
                            }
                            return;
                        });
                    }
                    return;
                });
            }
        );
    },
*/
    deleteZkNodeOfPath: function (path, cb) {
        var url = $.url(restUrl, "/zookeeper/deleteZkNodeOfPath?path="+path);
        request.get({url: url, forever: true}, $.resWrapper(cb));
    },

    loadLevelOfPath: function (path, cb) {
        var url = $.url(restUrl, "/zookeeper/loadLevelOfPath?path="+path);
        request.get({url: url, forever: true}, $.resWrapper(cb));
    },

    searchNodeOfValue: function(searchValue, selectPath, cb){
        var url= $.url(restUrl, "/zookeeper/searchNodeOfValue");
        console.log("zkService.js test");
        request.get({url: url, qs:{searchValue:searchValue,selectPath:selectPath}, forever: true}, $.resWrapper(cb));
    },
/*
    getZkNodeOfPath: function (path, cb) {
        var url = $.url(restUrl, "/zookeeper/getZkNodeOfPath?path="+path);
        request.get({url: url, forever: true}, $.resWrapper(cb));
    },
    */
};
