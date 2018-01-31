package com.creditease.dbus.commons;


/**
 * Created by dongwang47 on 2018/1/5.
 */
public class DbusMessage12 extends DbusMessage {
    public DbusMessage12() {
    }

    public DbusMessage12(String version, ProtocolType type, String schemaNs) {
        super(version, type);
        this.schema = new Schema12(schemaNs);
    }

    public static class Schema12 extends Schema {
        public Schema12() {
        }
        public Schema12(String schemaNs) {
            super(schemaNs);
        }
    }
}
