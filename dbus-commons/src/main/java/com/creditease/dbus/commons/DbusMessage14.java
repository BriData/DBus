package com.creditease.dbus.commons;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;
import java.util.List;

/**
 * Created by ximeiwang on 2018/1/9.
 */
public class DbusMessage14 extends DbusMessage13 {
    public DbusMessage14(String version, ProtocolType type, String schemaNs, int batchNo) {
        super(version, type, schemaNs,batchNo);
        this.schema = new Schema14(schemaNs, batchNo);
    }

    public DbusMessage14(String version, ProtocolType type, String schemaNs, int batchNo, List<Field> unsetField) {
        super(version, type, schemaNs,batchNo);
        this.schema = new Schema14(schemaNs, batchNo, unsetField);
    }
/*
    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
*/
    public static class Schema14 extends Schema13 {

        private List<Field> unFields;

        public Schema14(String schemaNs, int batchNo) {
            super(schemaNs,batchNo);
            //this.batchId = batchNo;
        }
        public Schema14(String schemaNs, int batchNo,List<Field> unsetField) {
            super(schemaNs,batchNo);
            this.unFields = unsetField;
        }

        public List<Field> getUnFields() {
        return unFields;
        }

        public void setUnFields(List<Field> unFields) {
            this.unFields = unFields;
        }

        /*
        public void addUnField(String name, DataType type, boolean nullable) {
            //index.put(name, this.unFields.size());
            setIndex(name, this.unFields.size());
            this.unFields.add(new Field(name, type, nullable));
        }*/
    }
}
