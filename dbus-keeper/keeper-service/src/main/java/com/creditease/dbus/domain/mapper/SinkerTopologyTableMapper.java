package com.creditease.dbus.domain.mapper;

import com.creditease.dbus.domain.model.SinkerTopologyTable;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface SinkerTopologyTableMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(SinkerTopologyTable record);

    SinkerTopologyTable selectByPrimaryKey(Integer id);

    int updateByPrimaryKey(SinkerTopologyTable record);

    List<SinkerTopologyTable> search(Map<String, Object> param);

    List<SinkerTopologyTable> searchAll(@Param("schemaId") Integer schemaId);

    int insertMany(@Param("list") List<SinkerTopologyTable> tables);
}