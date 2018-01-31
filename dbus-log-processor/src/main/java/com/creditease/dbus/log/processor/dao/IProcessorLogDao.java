package com.creditease.dbus.log.processor.dao;

import com.creditease.dbus.log.processor.vo.DBusDataSource;
import com.creditease.dbus.log.processor.vo.RuleInfo;
import java.util.List;

/**
 * Created by Administrator on 2017/9/27.
 */
public interface IProcessorLogDao {

     DBusDataSource loadDBusDataSourceConf(String key, String dsName);

     List<RuleInfo> loadActiveTableRuleInfo(String key, String dsName);

     void updateTableStatus(String key, String dsName, String schemaName, String tableName, String status);

     List<RuleInfo> loadAbortTableRuleInfo(String key, String dsName);

}
