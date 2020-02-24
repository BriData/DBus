package com.creditease.dbus.service;

import com.creditease.dbus.domain.mapper.SinkerTopologyTableMapper;
import com.creditease.dbus.domain.model.SinkerTopologyTable;
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

@Service
public class SinkerTableService {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private SinkerTopologyTableMapper sinkerTopologyTableMapper;

    public PageInfo<SinkerTopologyTable> search(Integer pageNum, Integer pageSize, String dsName, String schemaName, String tableName, String sinkerName) {
        Map<String, Object> map = new HashMap<>();
        ParamUtils.putNotNull(map, "dsName", dsName);
        ParamUtils.putNotNull(map, "schemaName", schemaName);
        ParamUtils.putNotNull(map, "tableName", tableName);
        ParamUtils.putNotNull(map, "sinkerName", sinkerName);
        PageHelper.startPage(pageNum, pageSize);
        return new PageInfo(sinkerTopologyTableMapper.search(map));
    }

    public List<SinkerTopologyTable> searchAll(Integer schemaId) {
        return sinkerTopologyTableMapper.searchAll(schemaId);
    }

    public int addSinkerTables(List<SinkerTopologyTable> addTables) {
        return sinkerTopologyTableMapper.insertMany(addTables);
    }

    public int delete(Integer id) {
        return sinkerTopologyTableMapper.deleteByPrimaryKey(id);
    }

    public int update(SinkerTopologyTable table) {
        return sinkerTopologyTableMapper.updateByPrimaryKey(table);
    }

}
