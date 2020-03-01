package com.creditease.dbus.service;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.SinkerTopologySchema;
import com.creditease.dbus.domain.model.SinkerTopologyTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class SinkerSchemaService {

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private RequestSender sender;
    @Autowired
    private SinkerService sinkerService;
    @Autowired
    private SinkerTableService sinkerTableService;

    public ResultEntity search(String request) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/sinkerSchema/search", request).getBody();
    }

    public ResultEntity update(SinkerTopologySchema schema) {
        return sender.post(ServiceNames.KEEPER_SERVICE, "/sinkerSchema/update", schema).getBody();
    }

    public void delete(Integer id) {
        sender.get(ServiceNames.KEEPER_SERVICE, "/sinkerSchema/delete/{id}", id);
        logger.info("delete sinker topology schema success .{}", id);
    }

    public SinkerTopologySchema getById(Integer id) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/sinkerSchema/getById/{id}", id).getBody().getPayload(SinkerTopologySchema.class);
    }

    public ResultEntity searchAll(String request) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/sinkerSchema/searchAll", request).getBody();
    }

    public void addSinkerTables(Map<String, Object> param) {
        SinkerTopologySchema sinkerSchema = JSON.parseObject(JSON.toJSONString(param.get("sinkerSchema")), SinkerTopologySchema.class);
        List<SinkerTopologyTable> sinkerSchemaList = JSON.parseArray(JSON.toJSONString(param.get("sinkerTableList")), SinkerTopologyTable.class);
        List<SinkerTopologyTable> addTables = sinkerSchemaList.stream()
                .filter(table -> table.getSinkerTopoId() == null)
                .map(table -> {
                    table.setSinkerTopoId(sinkerSchema.getSinkerTopoId());
                    table.setSinkerName(sinkerSchema.getSinkerName());
                    return table;
                }).collect(Collectors.toList());
        if (addTables != null && !addTables.isEmpty()) {
            Integer count = sinkerTableService.addSinkerTables(addTables);
            if (addTables.size() != count) {
                throw new RuntimeException("添加sinker表失败.");
            }
        }
    }

    public int addSinkerSchemas(List<SinkerTopologySchema> addSchemas) {
        return sender.post(ServiceNames.KEEPER_SERVICE, "/sinkerSchema/addSchemas", addSchemas).getBody().getPayload(Integer.class);
    }

    public List<SinkerTopologySchema> getBySinkerName(String sinkerName) {
        SinkerTopologySchema[] payload = sender.get(ServiceNames.KEEPER_SERVICE, "/sinkerSchema/searchNotPage", "?sinkerName=" + sinkerName).getBody()
                .getPayload(SinkerTopologySchema[].class);
        return payload == null ? null : Arrays.asList(payload);
    }

    public List<SinkerTopologySchema> selectAll() {
        SinkerTopologySchema[] payload = sender.get(ServiceNames.KEEPER_SERVICE, "/sinkerSchema/selectAll").getBody()
                .getPayload(SinkerTopologySchema[].class);
        return payload == null ? null : Arrays.asList(payload);
    }

    public void batchAddSinkerTables(List<SinkerTopologySchema> schemas) {
        schemas.forEach(schema -> {
            SinkerTopologyTable[] sinkerTopologyTables = sinkerTableService.searchAll("?schemaId=" + schema.getSchemaId()).getPayload(SinkerTopologyTable[].class);
            List<SinkerTopologyTable> addTables = Arrays.stream(sinkerTopologyTables).filter(table -> table.getSinkerTopoId() == null)
                    .map(table -> {
                        table.setSinkerTopoId(schema.getSinkerTopoId());
                        table.setSinkerName(schema.getSinkerName());
                        return table;
                    }).collect(Collectors.toList());
            if (addTables != null && !addTables.isEmpty()) {
                Integer count = sinkerTableService.addSinkerTables(addTables);
                if (addTables.size() != count) {
                    throw new RuntimeException("添加sinker表失败.");
                }
            }
        });
    }

    public void batchDeleteSinkerSchema(List<Integer> ids) {
        ids.forEach(id -> delete(id));
    }
}
