var express = require('express');
var router = express.Router();
var service = require('../lib/service/jar-manager-service');
var logger = require('../lib/utils/logger');
var util = require('util');
var fs = require('fs');
var multiparty = require('multiparty');
var child_process = require('child_process');
var config = require('../config');

router.get('/listJar', function (req, res) {
    try {
        service.listJars(function listJar(err, data) {
            if (err) {
                res.json({status: -1, message: "List jar files error"});
                return;
            }
            var data = data;
            var desc = data[0].data;
            var jarList = data[0].jarList;
            var jarInfo = [];
            var i = 1;
            var start = 0;
            var end;
            jarList.forEach(function (e) {
                end = desc.indexOf("\n" + i + "\n");
                jarInfo.push({path:e,desc:desc.substring(start,end)});
                start = end + 3 ;
                i++;
            });
            res.json({status: 200, data: jarInfo});

        });
    } catch (error) {
        logger.error("Error occurred while process list jar :%j.\n Error message -- %s \n%s", "", error.message, error.stack);
    }
});

router.get('/modifyDesc', function (req, res) {
    try {
        var path = req.query["path"];
        var description = req.query["description"];
        var params = {
            path:path,
            description:description
        };
        service.modifyDesc(params,function modifyDesc(err, data) {
            if (err) {
                res.json({status: -1, message: "modify jar description error"});
                return;
            }
            console.log("data: " + data);
            res.json({status: 200, data: ""});

        });
    } catch (error){
        logger.error("Error occurred while process modify jar description :%j.\n Error message -- %s \n%s", "", error.message, error.stack);
    }
});

router.post('/upload', function(req, res){
    var dir = __dirname + "/../files/";
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir);
    }
    //生成multiparty对象，并配置上传目标路径
    var form = new multiparty.Form({uploadDir: dir});
    //上传完成后处理
     form.parse(req, function(err, fields, files) {
          if(err){
              console.log('parse error: ' + err);
          } else {
             var inputFile = files.file[0];
             var index = inputFile.path.lastIndexOf('/');
             var randomName = inputFile.path.substring(index + 1);
             var originName =  inputFile.originalFilename;
             var methods = [];
             var port = config.sshConfig.port;
             var target_dir =  " " + config.sshConfig.user + "@" + config.sshConfig.host + ":" + config.sshConfig.baseDir;
             //这里只是将文件copy到dbusssh的家目录下，可以将文件放入指定目录
             //var cmd = "scp -P "+ port +" " + inputFile.path+ " dbusssh@localhost:/home/dbusssh";
             var cmd = "scp -P "+ port +" " + inputFile.path + target_dir;
             var params = {
                 originName:originName,
                 randomName:randomName
             };
              child_process.exec(cmd,function (err,stdout,stderr) {
                  if(err) {
                      logger.info("err: " + err);
                      return;
                  } else {
                      //对文件重命名
                      service.rename(params,function rename(err, out) {
                          if(err)
                          {                      
                              logger.info("rename err: " + err);
                              return;
                          } else {
                              logger.info("rename ok " );
                              res.end();
                          }
                      });
                  }

              });
         }
     });
});

module.exports = router;
