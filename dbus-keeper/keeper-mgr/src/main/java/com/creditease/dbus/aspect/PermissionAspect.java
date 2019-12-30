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


package com.creditease.dbus.aspect;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ProjectStatus;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.service.ProjectService;
import com.creditease.dbus.service.ProjectUserService;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

/**
 * User: 王少楠
 * Date: 2018-06-27
 * Time: 上午11:52
 */
@Component
@Aspect
public class PermissionAspect extends BaseController {

    @Autowired
    private ProjectUserService projectUserService;

    @Autowired
    private ProjectService projectService;

    @Pointcut("@annotation(com.creditease.dbus.annotation.AdminPrivilege) " +
            "and execution(public * com.creditease.dbus.controller.*.*(..))")
    public void adminPrivilege() {
    }

    @Pointcut("@annotation(com.creditease.dbus.annotation.ProjectAuthority) " +
            "and execution(public * com.creditease.dbus.controller.*.*(..))")
    public void projectAuthority() {
    }

    @Around("adminPrivilege()")
    public Object permissionAdminValidate(ProceedingJoinPoint pJointPoint) {
        String role = currentUserRole();
        if (!StringUtils.equalsIgnoreCase(role, "admin")) {
            return resultEntityBuilder().status(MessageCode.AUTHORIZATION_FAILURE).build();
        } else {
            try {
                return pJointPoint.proceed();
            } catch (Throwable e) {
                logger.error("[adminPrivilege] 接口发生异常:{}", e);
                return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
            }

        }
    }

    @Around("projectAuthority()")
    public Object permissionProjectValidate(ProceedingJoinPoint pJointPoint) throws Exception {

        boolean permission = false;//是否有权限访问project

        Integer userId = null; //外面定义,是为了打印日志
        Integer projectId = null;
        //管理员具有权限,不进行限制
        if (StringUtils.equalsIgnoreCase(KeeperConstants.USER_ROLE_TYPE_ADMIN, currentUserRole())) {
            permission = true;
        } else {
            //当前用户id
            userId = currentUserId();
            if (userId == null) {
                //用户Id为空,没有权限访问接口
                logger.info("[projectAuthority] userId is null.");
                return resultEntityBuilder().status(MessageCode.AUTHORIZATION_FAILURE).build();
            }

            //获取参数中的projectId
            projectId = null;
            try {
                projectId = getProjectId(pJointPoint);
                if (projectId == null)
                    throw new Exception("projectId not found.");
            } catch (Exception e) {
                logger.error("[projectAuthority] projectId get error: Exception:{}", e);
                return resultEntityBuilder().status(MessageCode.PROJECT_ID_EMPTY).build();
            }

            /*根据projectId获取project下所有用户,然后判断用户是否有访问project的权利*/
            ResponseEntity<ResultEntity> response = projectUserService.getUsersByProIdToResponse(projectId);
            if (!response.getStatusCode().is2xxSuccessful() || !response.getBody().success())
                return response.getBody();
            List<Map<String, Object>> projectUsers = response.
                    getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {
            });


            for (Map<String, Object> projectUser : projectUsers) {
                Integer pUserId = (Integer) projectUser.get("id");
                if (pUserId.intValue() == userId.intValue()) {
                    permission = true;
                    break;
                }
            }
            //项目过期限制
            ResultEntity result = projectService.queryProjectId(projectId);
            if (result.getStatus() != ResultEntity.SUCCESS) {
                return result;
            } else {
                Project project = result.getPayload(new TypeReference<Project>() {
                });
                if (ProjectStatus.INACTIVE.getStatus().equals(project.getStatus())) {
                    logger.info("[projectAuthority] project inactive. projectName:{}", project.getProjectName());
                    return resultEntityBuilder().status(MessageCode.PROJECT_INACTIVE).build();
                }
            }
        }

        //根据权限判断,是否可以访问接口.  管理员属于特殊权限
        if (permission) {
            try {
                //访问接口
                //return (ResultEntity) pJointPoint.proceed();
                return pJointPoint.proceed();
            } catch (Throwable e) {
                logger.error("[projectAuthority] 接口发生异常:{}", e);
                return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
            }
        } else {
            logger.info("[projectAuthority] 用户没有权限访问project. userId:{},projectId:{}.", userId, projectId);
            return resultEntityBuilder().status(MessageCode.AUTHORIZATION_FAILURE_PROJECT).build();
        }
    }

    private Integer getProjectId(ProceedingJoinPoint pJointPoint) throws Exception {
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        //如果在param中有projectId字段,取该字段
        String projectStr = request.getParameter("projectId");
        if (StringUtils.isNotEmpty(projectStr)) {
            return Integer.valueOf(projectStr);
        } else {
            /*param中没有的话,就从路径中取,会有以下三种情况
            1. /delete/{projectId} 只有一个参数,这个参数就是projectId
            2./update/{projectId}/{tableId}   多个int参数,第一个int值
            3./update/{name}/{projectId}  多个非int参数,第一个int值

            总结：
            对于情况1,直接获取这个路径参数即可,此时唯一的路径参数对应唯一的函数参数,直接取函数参数即可。
            对于情况2、3,取路径参数中第一个int值为projectId
            */
            Object[] args = pJointPoint.getArgs();
            if (args.length < 1) {
                throw new Exception("projectId not found");
            } else if (args.length == 1) {
                return Integer.valueOf(args[0].toString());
            } else {
                String uri = request.getRequestURI();
                String[] pathVariables = uri.split("/");
                Integer proId = null;
                for (String pathVar : pathVariables) {
                    try {
                        proId = Integer.valueOf(pathVar);
                        break;
                    } catch (Exception e) {
                        continue;
                    }
                }
                return proId;
            }
        }

    }

}
