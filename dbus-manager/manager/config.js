/**
* Created by zhangyf on 16/9/20.
*/
var ConfUtils = require('./lib/utils/ConfUtils.js');
var zkconfig = {
    //从zk集群中选择一台配置。
    //新测试环境
    connect: ConfUtils.zookeeperServers(),


    timeout: 200000,
    debug_level: 'WARN',
    host_order_deterministic: false,

    client: {
        //Session timeout in milliseconds, defaults to 30 seconds.
        sessionTimeout: 1000,
        //The delay (in milliseconds) between each connection attempts.
        spinDelay: 0,
        //The number of retry attempts for connection loss exception.
        retries: 3
    }
};

var restConfig = {
    //dbusRest最新环境
    dbusRest: "http://localhost:"+ConfUtils.webServicePort(),


    //storm UI 所在机器的配置
    // stormRest: "http://localhost:8080/api/v1"

};

var loggerConfig = {
    level: 'debug',
    logdir: __dirname + '/logs'
};

//storm nimbus所在机器配置。
// jar包所在机器,一般将jar文件放在/home/{用户名}目录下。
var sshConfig = {
};

var config = {
    zk: zkconfig,
    rest: restConfig,
    loggerConfig: loggerConfig,
    sshConfig: sshConfig
};

module.exports = config;
