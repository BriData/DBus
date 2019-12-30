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


package com.creditease.dbus.stream.oracle.appender.avro;

import com.creditease.dbus.stream.common.Constants;
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

/**
 * osource generic schema decoder
 * Created by Shrimp on 16/5/5.
 */
public class GenericSchemaDecoder {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private GenericDatumReader<GenericRecord> datumReader;
    private static GenericSchemaDecoder instance = new GenericSchemaDecoder();

    private GenericSchemaDecoder() {
        initDecoder(Constants.GENERIC_SCHEMA);
    }

    private void initDecoder(String fileName) {
        try {
            Schema genericSchema = SchemaProvider.getInstance().getSchema(fileName);
            datumReader = new GenericDatumReader<>(genericSchema);
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
        List<GenericRecord> list = new LinkedList<>();
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = getBinaryDecoder(payload);
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
                schemaBean.setSchemaHash((Integer) record.get(GenericData.SCHEMA_HASH));
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
