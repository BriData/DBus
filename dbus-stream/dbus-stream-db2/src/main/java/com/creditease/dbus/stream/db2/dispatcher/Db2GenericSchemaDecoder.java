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


package com.creditease.dbus.stream.db2.dispatcher;

import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.common.tools.IGenericMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * osource generic schema getInstance
 * Created by Shrimp on 16/5/5.
 */
public class Db2GenericSchemaDecoder {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Schema genericSchema;
    private SchemaRegistryClient schemaRegistry;

    private GenericDatumReader<GenericRecord> datumReader;
    private GenericDatumWriter<GenericRecord> datumWriter;
    private static Db2GenericSchemaDecoder instance = new Db2GenericSchemaDecoder();

    private Db2GenericSchemaDecoder() {
        initDecoder();
    }

    public static Db2GenericSchemaDecoder getInstance(String restUrl) {
        instance.schemaRegistry = new CachedSchemaRegistryClient(restUrl, 20, null);
        return instance;
    }

    private void initDecoder() {
        try {
            genericSchema = Db2GenericSchemaProvider.getInstance().getSchema("db2_generic_wrapper.avsc");
            datumReader = new GenericDatumReader<>(genericSchema);
            datumWriter = new GenericDatumWriter<>(genericSchema);
        } catch (Exception e) {
            logger.error("Db2GenericSchemaDecoder Initialization Error!", e);
            e.printStackTrace();
        }
    }

    /**
     * 解析被generic schema封装的实际数据
     *
     * @param schema  schema对象
     * @param payload 实际数据
     * @return List<GenericRecord>
     * @throws Exception
     */
    public List<GenericRecord> decode(Db2GenericMessage msg) throws IOException {

        ByteBuffer buffer = ByteBuffer.wrap(msg.getPayload());

        if (buffer.get() != Constants.MAGIC_BYTE) {
            logger.error("Unknown magic byte!");
        }

        int id = buffer.getInt();

        Schema schema = null;
        try {
            schema = ThreadLocalCache.get(Constants.CacheNames.TABLE_SCHEMA_VERSION_CACHE, Utils.buildDataTableSchemaCacheKey(msg.getSchemaName(), msg.getTableName(), String.valueOf(id)));
            if (schema == null) {
                schema = schemaRegistry.getById(id);
                ThreadLocalCache.put(Constants.CacheNames.TABLE_SCHEMA_VERSION_CACHE, Utils.buildDataTableSchemaCacheKey(msg.getSchemaName(), msg.getTableName(), String.valueOf(id)), schema);
            }

        } catch (RestClientException e) {
            logger.error("Schema Registry RestClientException: " + e);
        }

        int length = buffer.limit() - 5;
        int start = buffer.position() + buffer.arrayOffset();

//        logger.debug("Schema:" + schema.toString() + " schema payload:" + new String(msg.getPayload(), "utf-8"));
        List<GenericRecord> list = new LinkedList<>();
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(buffer.array(), start, length, null);
        GenericRecord genericRecord = reader.read(null, decoder);
        list.add((GenericRecord) genericRecord);

        return list;
    }


    public List<GenericRecord> decodeFullPull(Db2GenericMessage msg) throws IOException {
        return decode(msg);
    }

    public List<GenericRecord> decodeHeartBeat(Db2GenericMessage msg) throws IOException {
        return decode(msg);
    }

    public byte[] wrap(List<IGenericMessage> list) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().blockingBinaryEncoder(outputStream, null);

        for (IGenericMessage obj : list) {
            Db2GenericMessage msg = (Db2GenericMessage) obj;
            GenericRecord record = msg.generateRecord(genericSchema);
            datumWriter.write(record, encoder);
        }
        encoder.flush();

        return outputStream.toByteArray();
    }

    public List<IGenericMessage> unwrap(DBusConsumerRecord record) throws IOException {
        List<IGenericMessage> list = new LinkedList<>();

        String[] schemaTableInfo = StringUtils.split(record.topic(), '.');
        if (schemaTableInfo.length != 3) {
            logger.error("record topic is error: [" + record.topic() + "]");
        }
        String dbSchema = schemaTableInfo[1];
        String tableName = schemaTableInfo[2];

        if (record.value() == null) {
            logger.error("DBusConsumerRecord record.value() is null!");
        } else {
            ByteBuffer buffer = ByteBuffer.wrap((byte[]) record.value());

            if (buffer.get() != Constants.MAGIC_BYTE) {
                logger.error("Unknown magic byte!");
            }

            int id = buffer.getInt();
            Schema schema = null;
            try {
                //这里先去cache中查询数据的schema，如果cache中没有，则再去schema registry中查询，并写入cache中
                schema = ThreadLocalCache.get(Constants.CacheNames.TABLE_SCHEMA_VERSION_CACHE, Utils.buildDataTableSchemaCacheKey(dbSchema, tableName, String.valueOf(id)));
                if (schema == null) {
                    schema = schemaRegistry.getById(id);
                    ThreadLocalCache.put(Constants.CacheNames.TABLE_SCHEMA_VERSION_CACHE, Utils.buildDataTableSchemaCacheKey(dbSchema, tableName, String.valueOf(id)), schema);
                }
            } catch (RestClientException e) {
                logger.error("Schema Registry RestClientException: " + e);
            }

            int length = buffer.limit() - 5;
            int start = buffer.position() + buffer.arrayOffset();


            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(buffer.array(), start, length, null);
            GenericRecord genericRecord = reader.read(null, decoder);

            if (!StringUtils.equals(Constants.DB2MessageBodyKey.RR, (CharSequence) genericRecord.get(Constants.DB2MessageBodyKey.DB2_ENTTYP))) {
                Db2GenericMessage msg = new Db2GenericMessage();
                String[] arrList = StringUtils.split(schema.getFullName(), ".");
                msg.setNameSpace(StringUtils.joinWith(".", arrList[2], arrList[3]));
                msg.setPayload((byte[]) record.value());
                msg.setSchemaId(id);
                msg.setOffset(String.valueOf(record.offset()));

                list.add(msg);
            }
        }

        return list;
    }


    private BinaryDecoder getBinaryDecoder(byte[] bytes) {
        return DecoderFactory.get().binaryDecoder(bytes, null);
    }
}
