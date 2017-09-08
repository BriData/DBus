/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.stream.mysql.appender.bolt.processor.appender;

import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaVerController;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.EntryHeader;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.MessageEntry;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 处理mysql 同步meta
 * @author xiongmao
 *
 */
public class MaMetaSyncEventHandler implements BoltCommandHandler {
	private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;

    public MaMetaSyncEventHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }
    
    @Override
    public void handle(Tuple tuple) {
    	try{
    		EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
    		List<MessageEntry> datas = emitData.get(EmitData.MESSAGE);
    		long offset = emitData.get(EmitData.OFFSET);

            logger.debug("[BEGIN] receive data,offset:{}", offset);
            
            for(MessageEntry msgEntry : datas){
            	long pos = Long.parseLong(msgEntry.getEntryHeader().getPos());
            	syncMeta(msgEntry, pos, offset, tuple);
            }
    	} catch (Exception e){
    		logger.error("Error when processing data", e);
            throw new RuntimeException(e);
    	}
    }
    
    private void syncMeta(MessageEntry msgEntry, long pos, long offset, Tuple input) throws Exception {
    	logger.info("Received a meta sync message {}",msgEntry.getSql());
    	//处理表结构变化消息,包含truncate和alter
    //	String ddlType = StringUtils.substringBefore(msgEntry.getSql(), " ").toUpperCase();
    	EntryHeader header = msgEntry.getEntryHeader();
        if (header.isTruncate()) {
            MetaVersion version = MetaVerController.getSuitableVersion(header.getSchemaName(), header.getTableName(), pos, offset);
            if (version == null) {
                logger.warn("The version of table {}.{} was not found.", header.getSchemaName(), header.getTableName());
                return;
            }

            // 生成版本号之前发送termination消息
            //sendTermination(version);

            // truncate处理为meta namespace变更,wormhole团队可以容易处理
            // 改变外部版本号,外部版本号变化后wormhole会根据版本号分区相同表的数据
            version.nextVer();
            MetaVerController.updateVersion(version);

            logger.info(String.format("Message version changed[schema:%s, table:%s, type:%s]", header.getSchemaName(), header.getTableName(), header.getOperType().name()));
        } else if (header.isAlter()) {
            MetaVersion version = MetaVerController.getVersionFromCache(header.getSchemaName(), header.getTableName());
            if (version == null) {
                logger.warn("The version of table {}.{} was not found.", header.getSchemaName(), header.getTableName());
                return;
            }

            if (version != null && version.getTrailPos() < pos) {
                BoltCommandHandlerHelper.setMetaChangeFlag(header.getSchemaName(), header.getTableName());
                //version.setNeedCompare(true);
                /*MetaWrapper meta = version.getMeta();
        		List<MetaCell> cells = meta.getColumns();
        		for(MetaCell cell : cells){
        			Timestamp ts = new Timestamp(header.getExecuteTime());
        			cell.setDdlTime(ts);
        		}*/
        	}else {
                logger.info("version != null && version.getTrailPos() < pos --> false");
            }
        }
    }
    
    public static void main(String[] args){
    	Date date = new Date();
    	DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        System.out.println(format.format(date));
    	
    	Timestamp ts = new Timestamp(1473755543000L);
        System.out.println(ts.toString());
    	date = ts;
        System.out.println(format.format(date));
    }
}
