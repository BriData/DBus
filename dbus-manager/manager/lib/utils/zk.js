/**
 * zookeeper连接工具
 * Created by zhangyf on 16/9/21.
 */
var ZooKeeper = require ('zookeeper');
var config = require('../../config');
var zk = new ZooKeeper(config.zk);

module.exports = {
    get: function(path, callback) {
        zk.connect(function(err){
            if(err) callback(err);
            zk.a_get(path, null, function(rc, err, status, data) {
                callback(err, data);
            });
        });
    },
    set: function(path, callback) {
    }
}
