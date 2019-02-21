package com.creditease.dbus.controller;

import com.creditease.dbus.annotation.AdminPrivilege;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
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
	 * @param topoTableId
	 * @param tableId
	 * @param outputTopic
	 * @param splitShardSize
	 * @param splitCol
	 * @param splitShardStyle
	 * @param inputConditions
	 * @return
	 */
	@GetMapping("/globalfullPull")
	public ResultEntity globalfullPull(Integer topoTableId, Integer tableId, @RequestParam String outputTopic, String splitShardSize,
	                                   String splitCol, String splitShardStyle, String inputConditions) {
		try {
			int result = fullPullService.globalfullPull(topoTableId, tableId, outputTopic, splitShardSize, splitCol,
					splitShardStyle, inputConditions);
			return resultEntityBuilder().status(result).build();
		} catch (Exception e) {
			logger.error("Exception encountered while request globalfullPull Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@PostMapping("/batchGlobalfullPull")
	public ResultEntity batchGlobalfullPull(@RequestBody Map<String,Object> map) {
		try {
			batchFullPullService.batchGlobalfullPull(map);
			return resultEntityBuilder().build();
		} catch (Exception e) {
			logger.error("Exception encountered while request batchGlobalfullPull Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

}
