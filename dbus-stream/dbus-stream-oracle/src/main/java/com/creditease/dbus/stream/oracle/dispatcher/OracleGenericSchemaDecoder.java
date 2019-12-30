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


package com.creditease.dbus.stream.oracle.dispatcher;

import com.creditease.dbus.stream.common.tools.IGenericMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;
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
public class OracleGenericSchemaDecoder {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Schema genericSchema;
    private Schema fullPullSchema;
    private int fullPullHash;
    private Schema heartbeatSchema;
    private int heartbeatHash;
    private Schema syncEventSchema;
    private int syncEventHash;

    private GenericDatumReader<GenericRecord> datumReader;
    private GenericDatumWriter<GenericRecord> datumWriter;
    private static OracleGenericSchemaDecoder instance = new OracleGenericSchemaDecoder();

    private OracleGenericSchemaDecoder() {
        initDecoder();
    }

    public static OracleGenericSchemaDecoder getInstance() {
        return instance;
    }

    private void initDecoder() {
        try {
            genericSchema = OracleGenericSchemaProvider.getInstance().getSchema("generic_wrapper.avsc");

            fullPullSchema = OracleGenericSchemaProvider.getInstance().getSchema("DBUS.DB_FULL_PULL_REQUESTS.avsc");
            fullPullHash = OracleGenericSchemaProvider.getInstance().getSchemaHash("DBUS.DB_FULL_PULL_REQUESTS.avsc");

            syncEventSchema = OracleGenericSchemaProvider.getInstance().getSchema("DBUS.META_SYNC_EVENT.avsc");
            syncEventHash = OracleGenericSchemaProvider.getInstance().getSchemaHash("DBUS.META_SYNC_EVENT.avsc");

            heartbeatSchema = OracleGenericSchemaProvider.getInstance().getSchema("DBUS.DB_HEARTBEAT_MONITOR.avsc");
            heartbeatHash = OracleGenericSchemaProvider.getInstance().getSchemaHash("DBUS.DB_HEARTBEAT_MONITOR.avsc");

            datumReader = new GenericDatumReader<>(genericSchema);
            datumWriter = new GenericDatumWriter<>(genericSchema);
        } catch (Exception e) {
            logger.error("OracleGenericSchemaDecoder Initialization Error!", e);
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
    public List<GenericRecord> decode(Schema schema, byte[] payload) throws IOException {
        logger.debug("Schema:" + schema.toString() + " schema payload:" + new String(payload, "utf-8"));
        List<GenericRecord> list = new LinkedList<>();
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = getBinaryDecoder(payload);
        while (!decoder.isEnd()) {
            list.add(reader.read(null, decoder));
        }
        return list;
    }

    public List<GenericRecord> decodeSyncEvent(int hash, byte[] payload) throws IOException {
        if (syncEventHash != hash) {
            throw new RuntimeException(String.format("syncEvent schema hash 不一致, 期待的是%s, 实际是%s", syncEventHash, hash));
        }
        return decode(syncEventSchema, payload);
    }

    public List<GenericRecord> decodeFullPull(int hash, byte[] payload) throws IOException {
        if (fullPullHash != hash) {
            throw new RuntimeException(String.format("fullPull schema hash 不一致, 期待的是%s, 实际是%s", fullPullHash, hash));
        }
        return decode(fullPullSchema, payload);
    }

    public List<GenericRecord> decodeHeartBeat(int hash, byte[] payload) throws IOException {
        if (heartbeatHash != hash) {
            throw new RuntimeException(String.format("HeartBeat schema hash 不一致, 期待的是%s, 实际是%s", heartbeatHash, hash));
        }
        return decode(heartbeatSchema, payload);
    }

    public byte[] wrap(List<IGenericMessage> list) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().blockingBinaryEncoder(outputStream, null);

        for (IGenericMessage obj : list) {
            OracleGenericMessage msg = (OracleGenericMessage) obj;
            GenericRecord record = msg.generateRecord(genericSchema);
            datumWriter.write(record, encoder);
        }
        encoder.flush();

        return outputStream.toByteArray();
    }

    public List<IGenericMessage> unwrap(byte[] input) throws IOException {
        List<IGenericMessage> list = new LinkedList<>();

        BinaryDecoder decoder = getBinaryDecoder(input);
        while (!decoder.isEnd()) {
            GenericRecord record = datumReader.read(null, decoder);

            OracleGenericMessage msg = new OracleGenericMessage();

            Utf8 utf8 = (Utf8) record.get(OracleGenericMessage.NAMESAPCE);
            msg.setNameSpace(utf8.toString());
            msg.setSchemaHash((Integer) record.get(OracleGenericMessage.SCHEMA_HASH));
            ByteBuffer buffer = (ByteBuffer) record.get(OracleGenericMessage.PAYLOAD);
            msg.setPayload(buffer.array());

            logger.debug(String.format("TAble: %s, HASH: %d\n", msg.getNameSpace(), msg.getSchemaHash()));

            list.add((IGenericMessage) msg);
        }

        return list;
    }

    private BinaryDecoder getBinaryDecoder(byte[] bytes) {
        return DecoderFactory.get().binaryDecoder(bytes, null);
    }
}
