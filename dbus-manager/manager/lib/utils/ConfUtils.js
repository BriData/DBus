/**
 * Created by zhangyf on 17/8/1.
 */
var PropertiesReader = require('properties-reader');
var path = require('path');
var appConf = path.join(process.cwd(), "conf", "application.properties");
var properties = PropertiesReader(appConf);

var ConfUtils = {
    userName: properties.get('administrator').trim(),
    password: properties.get('password').trim(),

    httpPort: function () {
        return properties.get('manager.server.port');
    },
    webServicePort: function() {
        return properties.get('rest.server.port');
    },
    zookeeperServers: function() {
        return properties.get('zk.servers');
    }
}

module.exports = ConfUtils;
