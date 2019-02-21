package com.creditease.dbus.controller;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.ProjectTopoTable;
import com.creditease.dbus.service.FullPullService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/28
 */
@RestController
@RequestMapping("/fullpull")
public class FullPullController extends BaseController {

    @Autowired
    private FullPullService fullPullService;

    @PostMapping("/updateCondition")
    public ResultEntity updateFullpullCondition(@RequestBody ProjectTopoTable projectTopoTable) {
        try {
            return resultEntityBuilder().status(fullPullService.updateFullpullCondition(projectTopoTable)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request updateFullpullCondition Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
