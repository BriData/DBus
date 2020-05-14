package com.creditease.dbus.controller;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.SinkerTopology;
import com.creditease.dbus.service.SinkerService;
import org.apache.catalina.servlet4preview.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/sinker")
public class SinkerController extends BaseController {

    @Autowired
    private SinkerService service;

    @GetMapping(path = "/search")
    public ResultEntity search(HttpServletRequest request) {
        try {
            return service.search(request.getQueryString());
        } catch (Exception e) {
            logger.error("Exception encountered while search sinker topology", e);
            return new ResultEntity(MessageCode.EXCEPTION, e.getMessage());
        }
    }

    @PostMapping(path = "/create")
    public ResultEntity create(@RequestBody SinkerTopology sinkerTopology) {
        try {
            return service.create(sinkerTopology);
        } catch (Exception e) {
            logger.error("Exception encountered while create sinker topology", e);
            return new ResultEntity(MessageCode.EXCEPTION, e.getMessage());
        }
    }

    @PostMapping(path = "/update")
    public ResultEntity update(@RequestBody SinkerTopology sinkerTopology) {
        try {
            return service.update(sinkerTopology);
        } catch (Exception e) {
            logger.error("Exception encountered while update sinker topology", e);
            return new ResultEntity(MessageCode.EXCEPTION, e.getMessage());
        }
    }

    @GetMapping(path = "/delete/{id}")
    public ResultEntity delete(@PathVariable Integer id) {
        try {
            return service.delete(id);
        } catch (Exception e) {
            logger.error("Exception encountered while delete sinker topology", e);
            return new ResultEntity(MessageCode.EXCEPTION, e.getMessage());
        }
    }

    @GetMapping(path = "/searchById/{id}")
    public ResultEntity searchById(@PathVariable Integer id) {
        try {
            return resultEntityBuilder().payload(service.searchById(id)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while search sinker topology", e);
            return new ResultEntity(MessageCode.EXCEPTION, e.getMessage());
        }
    }

    @PostMapping(path = "/startOrStop")
    public ResultEntity startOrStop(@RequestBody Map<String, Object> param) {
        try {
            if (StringUtils.equals("start", (String) param.get("cmdType"))) {
                return resultEntityBuilder().payload(service.startSinkerTopology(param)).build();
            }
            if (StringUtils.equals("stop", (String) param.get("cmdType"))) {
                return service.stopSinkerTopology(param);
            }
            return new ResultEntity(MessageCode.EXCEPTION, "不支持的命令");
        } catch (Exception e) {
            logger.error("Exception encountered while start or stop sinker", e);
            return new ResultEntity(MessageCode.EXCEPTION, e.getMessage());
        }
    }

    @PostMapping(path = "/reload")
    public ResultEntity reload(@RequestBody SinkerTopology sinkerTopology) {
        try {
            service.reload(sinkerTopology);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while reload sinker topology", e);
            return new ResultEntity(MessageCode.EXCEPTION, e.getMessage());
        }
    }

    @GetMapping("/getSinkerTopicInfos")
    public ResultEntity getSinkerTopicInfos(@RequestParam String sinkerName) {
        try {
            return resultEntityBuilder().payload(service.getSinkerTopicInfos(sinkerName)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topic infos", e);
            return new ResultEntity(MessageCode.EXCEPTION, e.getMessage());
        }
    }

    @PostMapping(path = "/dragBackRunAgain")
    public ResultEntity dragBackRunAgain(@RequestBody Map<String, Object> param) {
        try {
            service.dragBackRunAgain(param);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while drag back run again", e);
            return new ResultEntity(MessageCode.EXCEPTION, e.getMessage());
        }
    }

    @PostMapping(path = "/addSinkerSchemas")
    public ResultEntity upsertMany(@RequestBody Map<String, Object> param) {
        try {
            service.addSinkerSchemas(param);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while add sinker topology schemas", e);
            return new ResultEntity(MessageCode.EXCEPTION, e.getMessage());
        }
    }

    @GetMapping(path = "view-log")
    public ResultEntity viewLog(@RequestParam String sinkerName) {
        try {
            return resultEntityBuilder().payload(service.viewLog(sinkerName)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while view log", e);
            return new ResultEntity(MessageCode.EXCEPTION, e.getMessage());
        }
    }

}
