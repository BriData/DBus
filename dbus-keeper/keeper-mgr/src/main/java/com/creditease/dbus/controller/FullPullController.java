/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package com.creditease.dbus.controller;

import com.creditease.dbus.annotation.AdminPrivilege;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.domain.model.ProjectTopoTable;
import com.creditease.dbus.service.BatchFullPullService;
import com.creditease.dbus.service.FullPullService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/28
 */
@RestController
@RequestMapping("/fullpull")
@AdminPrivilege
public class FullPullController extends BaseController {

    @Autowired
    private FullPullService fullPullService;
    @Autowired
    private BatchFullPullService batchFullPullService;

    /**
     * 更新租户表全量条件
     */
    @PostMapping("/updateCondition")
    public ResultEntity updateFullpullCondition(@RequestBody ProjectTopoTable projectTopoTable) {
        try {
            return fullPullService.updateFullpullCondition(projectTopoTable);
        } catch (Exception e) {
            logger.error("Exception encountered while request updateFullpullCondition Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 更新源端表全量条件
     */
    @PostMapping("/updateSourceCondition")
    public ResultEntity updateSourceFullpullCondition(@RequestBody DataTable dataTable) {
        try {
            return fullPullService.updateSourceFullpullCondition(dataTable);
        } catch (Exception e) {
            logger.error("Exception encountered while request updateSourceFullpullCondition task .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 这里根据 outputTopic不为空落kafka,hdfsRootPath不为空落hdfs
     */
    @GetMapping("/globalfullPull")
    public ResultEntity globalfullPull(Integer topoTableId, Integer tableId, String outputTopic, String splitShardSize,
                                       String splitCol, String splitShardStyle, String inputConditions, String hdfsRootPath) {
        try {
            int result = fullPullService.globalfullPull(topoTableId, tableId, outputTopic, splitShardSize, splitCol,
                    splitShardStyle, inputConditions, hdfsRootPath);
            return resultEntityBuilder().status(result).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request globalfullPull task .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping("/batchGlobalfullPull")
    public ResultEntity batchGlobalfullPull(@RequestBody Map<String, Object> map) {
        try {
            batchFullPullService.batchGlobalfullPull(map);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request batchGlobalfullPull task .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 断点续传
     *
     * @param id
     * @return
     */
    @GetMapping("/resumingFullPull/{id}")
    public ResultEntity resumingFullPull(@PathVariable Long id) {
        try {
            return fullPullService.resumingFullPull(id);
        } catch (Exception e) {
            logger.error("Exception encountered while request resumingFullPull task .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 任务重跑
     *
     * @param id
     * @return
     */
    @GetMapping("/rerun/{id}")
    public ResultEntity rerun(@PathVariable Long id) {
        try {
            return fullPullService.rerun(id);
        } catch (Exception e) {
            logger.error("Exception encountered while request rerun task .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }
}
