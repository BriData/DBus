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


package com.creditease.dbus.heartbeat.type;

import com.alibaba.fastjson.JSON;

import java.util.List;

/**
 * Created by haowei6 on 2017/9/19.
 */
public class MaasMessage {
    private DataSource data_source;
    private String object_owner;
    private SchemaInfo schema_info;
    private String ddl_sql;
    private String message_type;

    public DataSource getData_source() {
        return data_source;
    }

    public void setData_source(DataSource data_source) {
        this.data_source = data_source;
    }

    public String getObject_owner() {
        return object_owner;
    }

    public void setObject_owner(String object_owner) {
        this.object_owner = object_owner;
    }

    public SchemaInfo getSchema_info() {
        return schema_info;
    }

    public void setSchema_info(SchemaInfo schema_info) {
        this.schema_info = schema_info;
    }

    public String getDdl_sql() {
        return ddl_sql;
    }

    public void setDdl_sql(String ddl_sql) {
        this.ddl_sql = ddl_sql;
    }

    public String getMessage_type() {
        return message_type;
    }

    public void setMessage_type(String message_type) {
        this.message_type = message_type;
    }

    public static class DataSource {
        private List<Server> server;
        private String instance_name;
        private String database_type;

        public List<Server> getServer() {
            return server;
        }

        public void setServer(List<Server> server) {
            this.server = server;
        }

        public String getInstance_name() {
            return instance_name;
        }

        public void setInstance_name(String instance_name) {
            this.instance_name = instance_name;
        }

        public String getDatabase_type() {
            return database_type;
        }

        public void setDatabase_type(String database_type) {
            this.database_type = database_type;
        }

        public static class Server {
            public String host;
            public String port;

            public String getHost() {
                return host;
            }

            public void setHost(String host) {
                this.host = host;
            }

            public String getPort() {
                return port;
            }

            public void setPort(String port) {
                this.port = port;
            }

            @Override
            public boolean equals(Object server) {
                if (this == server) return true;
                if (server == null) return false;
                if (getClass() != server.getClass()) return false;

                Server server1 = (Server) server;
                if (!host.equals(server1.host)) return false;
                return host.equals(server1.host) && port.equals(server1.port);
            }

            @Override
            public int hashCode() {
                int result = host.hashCode();
                result = 31 * result + host.hashCode();
                return result;
            }
        }
    }

    public static class SchemaInfo {
        private String table_name;
        private String table_comment;
        private List<Column> columns;

        public String getTable_name() {
            return table_name;
        }

        public void setTable_name(String table_name) {
            this.table_name = table_name;
        }

        public String getTable_comment() {
            return table_comment;
        }

        public void setTable_comment(String table_comment) {
            this.table_comment = table_comment;
        }

        public List<Column> getColumns() {
            return columns;
        }

        public void setColumns(List<Column> columns) {
            this.columns = columns;
        }

        public static class Column {
            private String column_name;
            private String column_type;
            private String nullable;
            private String column_comment;
            private String data_length;
            private String data_precision;
            private String data_scale;
            private String is_pk;
            private String pk_position;

            public String getColumn_name() {
                return column_name;
            }

            public void setColumn_name(String column_name) {
                this.column_name = column_name;
            }

            public String getColumn_type() {
                return column_type;
            }

            public void setColumn_type(String column_type) {
                this.column_type = column_type;
            }

            public String getNullable() {
                return nullable;
            }

            public void setNullable(String nullable) {
                this.nullable = nullable;
            }

            public String getColumn_comment() {
                return column_comment;
            }

            public void setColumn_comment(String column_comment) {
                this.column_comment = column_comment;
            }

            public String getData_length() {
                return data_length;
            }

            public void setData_length(String data_length) {
                this.data_length = data_length;
            }

            public String getData_precision() {
                return data_precision;
            }

            public void setData_precision(String data_precision) {
                this.data_precision = data_precision;
            }

            public String getData_scale() {
                return data_scale;
            }

            public void setData_scale(String data_scale) {
                this.data_scale = data_scale;
            }

            public String getIs_pk() {
                return is_pk;
            }

            public void setIs_pk(String is_pk) {
                this.is_pk = is_pk;
            }

            public String getPk_position() {
                return pk_position;
            }

            public void setPk_position(String pk_position) {
                this.pk_position = pk_position;
            }
        }
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
