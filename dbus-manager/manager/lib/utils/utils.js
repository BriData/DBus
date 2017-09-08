var logger = require('./logger');
var request = require('request');

String.prototype.startWith = function (sub) {
    return this.indexOf(sub) == 0;
};
String.prototype.endWith = function (sub) {
    return this.lastIndexOf(sub) == this.length - sub.length;
};

var utils = {

    isEmpty: function (obj) {
        for (var name in obj) {
            return false;
        }
        return true;
    },

    url: function (host, path, p) {
        while (host.endWith("/")) {
            host = host.substring(0, host.length - 1);
        }
        while (path.startWith("/")) {
            path = path.substring(1, path.length);
        }
        var params = [];
        if (p) {
            for (var key in p) {
                params.push([key, "=", p[key]].join(""));
            }
            return [host, path].join("/") + "?" + params.join("&");
        }

        return [host, path].join("/");
    },

    extend: function (dist, src, keys) {
        dist = dist || src;
        src = src || {};
        keys = keys || [];
        keys.forEach(function (key) {
            dist[key] = src[key];
        });
    },

    resWrapper: function (cb) {
        return function (error, response, body) {
            if (response) {
                logger.info("[http-request] Response[%d] from url: '%s', body:%j",
                    response.statusCode, response.request.href, response.body);
            }
            if (!error && response.statusCode == 200) {
                cb(null, response);
                return;
            } else if (error) {
                logger.error("[http-request] Error occurred while sending http request with parameters:%j.\n%s \n%s",
                    //options,
             error.message, error.stack);
                cb(error);
            } else {
                cb(new Error(response.statusText, response.statusCode), response);
            }
        }
    },

    promiseHttp: {
        get: function (url, p, key, timeout) {
            var options = {url: url, forever: true};
            if (p) {
                options.qs = p;
            }
            options.timeout = timeout ? timeout : 20000;

            return new Promise(function (resolve, reject) {
                request(options, function (err, res) {
                    try {
                        requestCallback(resolve, reject, options, err, res, key);
                    } catch (e) {
                        logger.error("[http-request] Error occurred while sending http request with parameters:%j.\n%s \n%s", options, e.message, e.stack);
                        reject({error: e, response: res})
                    }
                });
            });
        },
        post: function (url, p, key, timeout) {
            var options = {url: url, forever: true};
            if (p) {
                options.json = p;
            }
            options.timeout = timeout ? timeout : 10000;
            return new Promise(function (resolve, reject) {
                request.post(options, function (err, res) {
                    try {
                        requestCallback(resolve, reject, options, err, res, key);
                    } catch (e) {
                        logger.error("[http-request] Error occurred while sending http request with parameters:%j.\n%s \n%s", options, e.message, e.stack);
                        reject({error: e, response: res})
                    }
                });
            });
        }
    }
};

function requestCallback(resolve, reject, options, error, response, key) {
    if (response) {
        logger.info("[http-request] Response[%d] from url: '%s', body:%j",
            response.statusCode, response.request.href, response.body);
    }
    if (!error && response.statusCode == 200) {
        if (key) {
            resolve({key: key, data: response.body});
        } else {
            resolve(response.body);
        }
        return;
    } else if (error) {
        logger.error("[http-request] Error occurred while sending http request with parameters:%j.\n%s \n%s",
            options, error.message, error.stack);
    }
    reject({err: error, response: response});
}

module.exports = utils;
