var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('./lib/utils/logger');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var session = require('express-session');
//var RedisStore = require('connect-redis')(session);
//var redis = require('./lib/utils/dbus-redis');
var routes = require('./routes/index');
var upload = require('jquery-file-upload-middleware');

//var routes = require('./routes/index');
//var verifyCode = require('./routes/verify-code');
var ds = require('./routes/datasource');

var dataSchema = require('./routes/data-schema');
var avroSchema = require('./routes/avro-schema');
var dataSchema2 = require('./routes/data-schema2');
var dataTable = require('./routes/data-table');
var stormTopology = require('./routes/storm-topology');
var startTopology = require('./routes/start-topology');
var jarManager = require('./routes/jar-manager');
var ctlMessage = require('./routes/ctl-message');
var tableMeta = require('./routes/table-meta');
var zk = require('./routes/zk');
var schemaTableInsert = require('./routes/schema-table-insert');
var fullPull = require('./routes/full-pull');
var config = require('./routes/conf');
var app = express();


app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false,limit:'100000kb',parameterLimit:'1000000'}));
app.use(cookieParser());

// 使用内存处理session
app.use(session({
    resave:true,
    saveUninitialized:true,
    secret: '89652fe6cb174ac75ca0552d52b7c6458', // dbus.creditease
    cookie: {}
}));

// 处理session,使用redis
//app.use(session({
//    store: new RedisStore({client: redis}),
//    secret: '89652fe6cb174ac75ca0552d52b7c6458', // dbus.creditease
//    resave: true,
//    saveUninitialized: true
//}));
app.use(require('./routes/login-filter'));
app.use('/', routes);
app.use('/logout', routes);

app.use('/ds', ds);
app.use('/schema', dataSchema);
app.use('/schema2', dataSchema2);
app.use('/avroSchema', avroSchema);
app.use('/tables', dataTable);
app.use('/tableMeta',tableMeta);
app.use('/topology', stormTopology);
app.use('/insertSchemaAndTable',schemaTableInsert);
app.use('/insertTablesInSource',schemaTableInsert);
app.use('/jarManager',jarManager);
app.use('/startTopology', startTopology);

//app.use('/verifyCode', verifyCode);
app.use('/zk', zk);
app.use('/ctlMessage', ctlMessage);
app.use('/fullPull',fullPull);
app.use('/config',config);
// catch 404 and forward to error handler
// app.use(function (req, res, next) {
//     var err = new Error('Resource Not Found');
//     err.status = 404;
//     next(err);
// });

// production error handler
// no stacktraces leaked to user
app.use(function (err, req, res, next) {
    res.json({status: err.status||500, message: err.message||"server internal error"});
    logger.error("Error occurred while request url: '%s' with method: '%s' params:%j \n%s", req.originalUrl, req.method, req.params, err.stack);
});

module.exports = app;
