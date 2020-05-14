package com.creditease.dbus.heartbeat.distributed.stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 新的格式有 10个字段
 *
 * 普通 data_increment_data.mysql.db1.schema1.table1.5.0.0.time.wh
 * 例子 data_increment_data.mysql.db1.schema1.table1.5.0.0.1481245701166.wh
 * 心跳 data_increment_heartbeat.mysql.db1.schema1.table1.5.0.0.time|txTime|ok.wh
 * 例子 data_increment_heartbeat.mysql.db1.schema1.table1.5.0.0.1481245701166|1481245700947|ok.wh
 */
public class HBKeySupplier implements Supplier<HBKeySupplier.HBKey>{

    private String key = null;

    public HBKeySupplier(String key) {
        this.key = key;
    }

    @Override
    public HBKey get() {
        String vals[] = StringUtils.split(".");
        if ( ArrayUtils.getLength(vals) != 10)
            return new HBKey(false);

        String type = vals[0];
        String dbType = vals[1];
        String ds = vals[2];
        String schema = vals[3];
        String table = vals[4];
        int partition = Integer.valueOf(vals[6]);

        String times[] = StringUtils.split(vals[8], "|");
        if (ArrayUtils.getLength(times) != 3)
            return new HBKey(false);

        long checkpointMs = Long.valueOf(times[0]);
        long txTimeMs = Long.valueOf(times[1]);
        String status = times[2];

        return new HBKey(type, dbType, ds, schema, table, partition, checkpointMs, txTimeMs, status, true, key);
    }

    public class HBKey {
        private String type;
        private String dbType;
        private String ds;
        private String schema;
        private String table;
        private int partition;
        private long checkpointMs;
        private long txTimeMs;
        private String status;
        boolean isNormalFormat;
        private String originKey;

        public HBKey(boolean isNormalFormat) {
            this.isNormalFormat = isNormalFormat;
        }

        public HBKey(String type, String dbType, String ds, String schema, String table, int partition, long checkpointMs, long txTimeMs, String status, boolean isNormalFormat, String key) {
            this.type = type;
            this.dbType = dbType;
            this.ds = ds;
            this.schema = schema;
            this.table = table;
            this.partition = partition;
            this.checkpointMs = checkpointMs;
            this.txTimeMs = txTimeMs;
            this.status = status;
            this.isNormalFormat = isNormalFormat;
            this.originKey = key;
        }

        public String getType() {
            return type;
        }

        public String getDbType() {
            return dbType;
        }

        public String getDs() {
            return ds;
        }

        public String getSchema() {
            return schema;
        }

        public String getTable() {
            return table;
        }

        public int getPartition() {
            return partition;
        }

        public long getCheckpointMs() {
            return checkpointMs;
        }

        public long getTxTimeMs() {
            return txTimeMs;
        }

        public String getStatus() {
            return status;
        }

        public boolean isNormalFormat() {
            return isNormalFormat;
        }

        public String getOriginKey() {
            return originKey;
        }
    }

}
