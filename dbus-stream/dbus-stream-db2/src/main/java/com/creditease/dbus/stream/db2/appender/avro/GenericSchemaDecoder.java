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


package com.creditease.dbus.stream.db2.appender.avro;

import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.common.Constants;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * osource generic schema decoder
 * Created by Shrimp on 16/5/5.
 */
public class GenericSchemaDecoder {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private GenericDatumReader<GenericRecord> datumReader;
    private static GenericSchemaDecoder instance = new GenericSchemaDecoder();
    private SchemaRegistryClient schemaRegistry;

    private GenericSchemaDecoder() {
        initDecoder(Constants.DB2_GENERIC_SCHEMA);
    }

    private void initDecoder(String fileName) {
        try {
            Schema genericSchema = SchemaProvider.getInstance().getSchema(fileName);
            datumReader = new GenericDatumReader<>(genericSchema);
            Properties props = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE);
            String restUrl = props.getProperty(Constants.ConfigureKey.SCHEMA_REGISTRY_REST_URL);
            schemaRegistry = new CachedSchemaRegistryClient(restUrl, 20, null);
        } catch (Exception e) {
            logger.error("GenericSchemaDecoder Initialization Error!", e);
        }
    }

    public static GenericSchemaDecoder decoder() {
        return instance;
    }

    /**
     * 解析被generic schema封装的实际数据
     *
     * @param schema  schema对象
     * @param payload 实际数据
     * @return List<GenericRecord>
     * @throws Exception
     */
    public List<GenericRecord> decode(Schema schema, byte[] payload) throws Exception {
        logger.trace("Schema:" + schema.toString() + " schema payload:" + new String(payload, "utf-8"));
//        List<GenericRecord> list = new LinkedList<>();
//        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
//        BinaryDecoder decoder = getBinaryDecoder(payload);
//        while (!decoder.isEnd()) {
//            list.add(reader.read(null, decoder));
//        }
//        return list;

        ByteBuffer buffer = ByteBuffer.wrap(payload);

        if (buffer.get() != Constants.MAGIC_BYTE) {
            logger.error("Unknown magic byte!");
        }

        int id = buffer.getInt();

        try {
            schema = schemaRegistry.getById(id);
        } catch (RestClientException e) {
            logger.error("Schema Registry RestClientException: " + e);
        }

        int length = buffer.limit() - 5;
        int start = buffer.position() + buffer.arrayOffset();

        logger.debug("Schema:" + schema.toString() + " schema payload:" + new String(payload, "utf-8"));
        List<GenericRecord> list = new LinkedList<>();
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(buffer.array(), start, length, null);

        GenericRecord genericRecord = reader.read(null, decoder);
        list.add((GenericRecord) genericRecord);

        return list;
    }

    /**
     * 解析被generic schema封装的实际数据
     *
     * @param schema  schema对象
     * @param payload 实际数据
     * @return List<GenericRecord>
     * @throws Exception
     */
    public List<GenericRecord> decode(Schema schema, byte[] payload, int start, int len) throws Exception {
        logger.trace("Schema:" + schema.toString() + " schema payload:" + new String(payload, "utf-8"));
        List<GenericRecord> list = new LinkedList<>();
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, start, len, null);
        while (!decoder.isEnd()) {
            list.add(reader.read(null, decoder));
        }
        return list;
    }

    /**
     * unwrap the osource generic schema
     *
     * @param input data
     * @return List<GenericData>
     * @throws Exception
     */
    public List<GenericData> unwrap(byte[] input) throws Exception {
        BinaryDecoder decoder = getBinaryDecoder(input);
        List<GenericData> list = new LinkedList<>();
        while (!decoder.isEnd()) {
            try {
                GenericRecord record = datumReader.read(null, decoder);
                GenericData schemaBean = new GenericData();
                Utf8 utf8 = (Utf8) record.get(GenericData.TABLE_NAME);
                schemaBean.setTableName(utf8.toString());
                schemaBean.setSchemaId((Integer) record.get(GenericData.SCHEMA_ID));
                Utf8 offset = (Utf8) record.get(GenericData.OFFSET);
                schemaBean.setOffset(offset.toString());
                ByteBuffer buffer = (ByteBuffer) record.get(GenericData.PAYLOAD);
                schemaBean.setPayload(buffer.array());
                list.add(schemaBean);
            } catch (Exception e) {
                throw e;
            }
        }
        logger.trace("message count:" + list.size());
        return list;
    }

    private BinaryDecoder getBinaryDecoder(byte[] bytes) {
        return DecoderFactory.get().binaryDecoder(bytes, null);
    }
}
