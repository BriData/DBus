var log = require('../../config').loggerConfig;

var fs = require('fs');
var winston = require('winston');
var moment = require('moment');
var stackTrace = require('stack-trace');
var DailyRotateFile = require('winston-daily-rotate-file');

var dateFormat = function () {
    return moment().format('YYYY-MM-DD HH:mm:ss:SSS');
};

// 判断log目录是否存在
if (!fs.existsSync(log.logdir)) {
    fs.mkdirSync(log.logdir);
}

var allLoggerTransport = new DailyRotateFile({
    name: 'all',
    filename: log.logdir + '/dbus-manager.log',
    timestamp: dateFormat,
    level: log.level,
    colorize: true,
    json: false,
    maxsize: 1024 * 1024 * 100,
    datePattern: '.yyyy-MM-dd'
});

var errorTransport = new (winston.transports.File)({
    name: 'error',
    filename: log.logdir + '/dbus-manager-error.log',
    timestamp: dateFormat,
    level: 'error',
    colorize: true,
    json: false
});

var logger = new (winston.Logger)({
    transports: [
        allLoggerTransport,
        errorTransport,
        new winston.transports.Console({
            timestamp: dateFormat,
            colorize: true,
            json: false
        })
    ]
});

// 崩溃日志
var crashLogger = new (winston.Logger)({
    transports: [
        new (winston.transports.File)({
            name: 'error',
            filename: log.logdir + '/error.log',
            level: 'error',
            handleExceptions: true,
            timestamp: dateFormat,
            humanReadableUnhandledException: true,
            json: false,
            colorize: true
        })
    ]
});

var methods = ["debug", "info", "warn", "error"];
methods.forEach(function (method) {
    var originMethod = logger[method];
    logger[method] = function () {
        var cellSite = stackTrace.get()[1];
        var args = [];
        for(var i = 0; i < arguments.length; i++) {
            args[i] = arguments[i];
        }
        args[0] = [cellSite.getFileName(), "[", cellSite.getLineNumber(), "] - ", arguments[0]].join("");
        originMethod.apply(logger, args);
    };
});

module.exports = logger;
