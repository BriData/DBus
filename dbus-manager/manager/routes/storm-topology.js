var express = require('express');
var router = express.Router();
var service = require('../lib/service/topology-service');
var stormApi = require('../lib/service/storm-rest');
var logger = require('../lib/utils/logger');
var util = require('util')

router.get('/list', function (req, res) {
    var param = helper.buildParam(req.query, ["dsId", "pageSize", "pageNum"]);
    if (!param.pageSize) {
        param.pageSize = 10;
    }
    if (!param.pageNum) {
        param.pageNum = 1;
    }
    //var promiseMethods = [stormApi.topologies(), service.search(param)];

    helper.stormTopologies().then(function (data) {
        var stormTopologyList = data.topologySummary.topologies;
        var fetcherMap = data.fetcherMap;
        var methods = [service.search(param)];
        for (var key in fetcherMap) {
            methods.push(fetcherMap[key]);
        }

        Promise.all(methods).then(function (dataList) {
            try {
                var dbusTopology = JSON.parse(dataList[0]); // search的返回结果
                dbusTopology.list.map(function (t) {
                    var elem = stormTopologyList.find(function (element) {
                        return element.name == t.topologyName;
                    });
                    if (elem) {
                        t.stormToplogyId = elem.id;
                        t.status = elem.status;
                        t.workers = helper.buildWorkers(dataList, elem);
                    }
                });
                service.listJars(function (err, jarList) {
                    if(err) {
                        res.json({status:-1, message:"List jar files error"});
                        return;
                    }
                    dbusTopology.list.map(function(elem) {
                        elem.jarList = jarList;
                        elem.selectedJar = elem.jarName;
                    });
                    res.json({status: 200, data: dbusTopology});
                });
            } catch (error) {
                logger.error("Error occurred while process topology list:%j.\n Error message -- %s \n%s", dataList, error.message, error.stack);
                res.json({status: -1, error: error.message});
            }

        }).catch(function (e) {
            res.json({status: -1, error: e.err.message || e.response.body});
        });
    }).catch(function (e) {
        res.json({status: -1, error: e.err.message || e.response.body});
    });
});

router.get('/selectList', function (req, res) {
    var param = helper.buildParam(req.query, ["dsId"]);
    //var promiseMethods = [stormApi.topologies(), service.search(param)];

    helper.stormTopologies().then(function (data) {
        var stormTopologyList = data.topologySummary.topologies;
        var fetcherMap = data.fetcherMap;
        var methods = [service.search(param)];
        for (var key in fetcherMap) {
            methods.push(fetcherMap[key]);
        }

        Promise.all(methods).then(function (dataList) {
            try {
                var dbusTopology = JSON.parse(dataList[0]); // search的返回结果
                dbusTopology.list.map(function (t) {
                    var elem = stormTopologyList.find(function (element) {
                        return element.name == t.topologyName;
                    });
                    if (elem) {
                        t.stormToplogyId = elem.id;
                        t.status = elem.status;
                        t.workers = helper.buildWorkers(dataList, elem);
                    }
                });
                service.listJars(function (err, jarList) {
                    if(err) {
                        res.json({status:-1, message:"List jar files error"});
                        return;
                    }
                    dbusTopology.list.map(function(elem) {
                        elem.jarList = jarList;
                        elem.selectedJar = elem.jarName;
                    });
                    res.json({status: 200, data: jarList});
                });
            } catch (error) {
                logger.error("Error occurred while process topology list:%j.\n Error message -- %s \n%s", dataList, error.message, error.stack);
                res.json({status: -1, error: error.message});
            }

        }).catch(function (e) {
            res.json({status: -1, error: e.err.message || e.response.body});
        });
    }).catch(function (e) {
        res.json({status: -1, error: e.err.message || e.response.body});
    });
});

router.get('/addTopology', function (req, res) {
    var param = helper.buildParam(req.query, [ "dsId", "jarName","topologyName"]);
    service.add(param,function addTopology(err,response) {
        if(err) {
            res.json({status: 500, message: err.message});
            return;
        }
        res.json({status: 200,id:response.body});
    });
});

var helper = {
    buildParam: function (query, params) {
        var param = {};
        params.forEach(function (key) {
            if (query[key]) {
                param[key] = query[key];
            }
        });
        return param;
    },
    stormTopologies: function () {
        return new Promise(function (resolve, reject) {
            stormApi.topologies().then(function (data) {
                var topologies = JSON.parse(data).topologies;
                var fetcherMap = {};
                topologies.forEach(function (t) {
                    fetcherMap[t.id] = stormApi.workers(t.id)
                });
                // 返回Topology信息和worker获取方法数组
                resolve({topologySummary: JSON.parse(data), fetcherMap: fetcherMap});
            }).catch(function (e) {
                reject(e);
            });
        });
    },
    buildWorkers: function (dataList, elem) {
        var workers;
        for (var i = 1; i < dataList.length; i++) {
            if (dataList[i].key === elem.id) {
                workers = JSON.parse(dataList[i].data).hostPortList;
                break;
            }
        }
        if (workers) {
            var result = [];
            workers.map(function (t) {
                result.push(t.host + ":" + t.port);
            });
            return result.join(",");
        }
        return null;
    }
}

module.exports = router;