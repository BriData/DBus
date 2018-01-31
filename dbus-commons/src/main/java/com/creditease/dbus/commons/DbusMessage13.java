package com.creditease.dbus.commons;

/**
 * Created by dongwang47 on 2018/1/5.
 */
public class DbusMessage13 extends DbusMessage {
    public DbusMessage13() {
    }

    public DbusMessage13(String version, ProtocolType type, String schemaNs, int batchNo) {
        super(version, type);
        this.schema = new Schema13(schemaNs, batchNo);
    }

    public static class Schema13 extends Schema {
        private int batchId;

        public Schema13() {
        }

        public Schema13(String schemaNs, int batchNo) {
            super(schemaNs);
            this.batchId = batchNo;
        }

        public int getBatchId() {
            return batchId;
        }
    }
}
