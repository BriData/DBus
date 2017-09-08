var SSH = require('simple-ssh');
var sshConfig = require('../../config').sshConfig;

module.exports = {
    promiseExec: function (command, config) {
        return new Promise(function (resolve, reject) {
            var ssh = new SSH(config||sshConfig);
            ssh.exec(command, {
                out: function (stdout) {
                    resolve(stdout);
                    ssh.end();
                },
                err: function (stderr) {
                    reject(stderr);
                    ssh.end();
                }
            }).start();
        });
    },

    exec: function (command, config, callback) {
        var ssh = new SSH(config||sshConfig);
        ssh.exec(command, {
            out: function (stdout) {
                callback(null, stdout);
                ssh.end();
            },
            err: function (stderr) {
                callback(stderr);
                ssh.end();
            }
        }).start();
    }
};
