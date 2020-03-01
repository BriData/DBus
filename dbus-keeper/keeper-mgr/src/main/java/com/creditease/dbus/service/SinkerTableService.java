package com.creditease.dbus.service;

import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.SinkerTopologyTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SinkerTableService {

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private RequestSender sender;

    public ResultEntity search(String request) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/sinkerTable/search", request).getBody();
    }

    public ResultEntity searchAll(String request) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/sinkerTable/searchAll", request).getBody();
    }

    public Integer addSinkerTables(List<SinkerTopologyTable> addTables) {
        return sender.post(ServiceNames.KEEPER_SERVICE, "/sinkerTable/addSinkerTables", addTables).getBody().getPayload(Integer.class);
    }

    public void delete(Integer id) {
        sender.get(ServiceNames.KEEPER_SERVICE, "/sinkerTable/delete/{id}", id);
        logger.info("delete sinker topology table success .{}", id);
    }

    public ResultEntity update(SinkerTopologyTable table) {
        return sender.post(ServiceNames.KEEPER_SERVICE, "/sinkerTable/update", table).getBody();
    }

    public void batchDeleteSinkerTable(List<Integer> ids) {
        ids.forEach(id -> delete(id));
    }
}
