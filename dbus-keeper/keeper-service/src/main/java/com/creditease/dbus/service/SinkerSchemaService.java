package com.creditease.dbus.service;

import com.creditease.dbus.domain.mapper.SinkerTopologySchemaMapper;
import com.creditease.dbus.domain.model.SinkerTopologySchema;
import com.creditease.dbus.util.ParamUtils;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class SinkerSchemaService {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private SinkerTopologySchemaMapper sinkerTopologySchemaMapper;

    public PageInfo<SinkerTopologySchema> search(Integer pageNum, Integer pageSize, String dsName, String schemaName, String sinkerName, String targetTopic) {
        Map<String, Object> map = new HashMap<>();
        ParamUtils.putNotNull(map, "dsName", dsName);
        ParamUtils.putNotNull(map, "schemaName", schemaName);
        ParamUtils.putNotNull(map, "sinkerName", sinkerName);
        ParamUtils.putNotNull(map, "targetTopic", targetTopic);
        PageHelper.startPage(pageNum, pageSize);
        return new PageInfo(sinkerTopologySchemaMapper.search(map));
    }

    public List<SinkerTopologySchema> searchNotPage(String dsName, String schemaName, String sinkerName, String targetTopic) {
        Map<String, Object> map = new HashMap<>();
        ParamUtils.putNotNull(map, "dsName", dsName);
        ParamUtils.putNotNull(map, "schemaName", schemaName);
        ParamUtils.putNotNull(map, "sinkerName", sinkerName);
        ParamUtils.putNotNull(map, "targetTopic", targetTopic);
        return sinkerTopologySchemaMapper.search(map);
    }

    public int delete(Integer id) {
        return sinkerTopologySchemaMapper.deleteByPrimaryKey(id);
    }

    public SinkerTopologySchema getById(Integer id) {
        return sinkerTopologySchemaMapper.selectByPrimaryKey(id);
    }

    public List<SinkerTopologySchema> searchAll(String dsName, String schemaName, Integer sinkerTopoId) {
        List<SinkerTopologySchema> sinkerTopologySchemas = sinkerTopologySchemaMapper.searchAll(dsName, schemaName);
        return sinkerTopologySchemas.stream().filter(schema -> (schema.getSinkerTopoId() == null || schema.getSinkerTopoId() == sinkerTopoId)
                && !schema.getSchemaName().equalsIgnoreCase("dbus")).collect(Collectors.toList());
    }

    public int addSchemas(List<SinkerTopologySchema> sinkerSchemaList) {
        return sinkerTopologySchemaMapper.insertMany(sinkerSchemaList);
    }

    public int update(SinkerTopologySchema schema) {
        return sinkerTopologySchemaMapper.updateByPrimaryKey(schema);
    }

    public List<SinkerTopologySchema> selectAll() {
        return sinkerTopologySchemaMapper.selectAll();
    }
}
