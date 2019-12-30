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

/**
 * Created by dashencui on 2017/12/13.
 */
public class ReceivedMaasMessage {
    private Data_Source data_source;
    private String owner;
    private String object_name;

    public Data_Source getData_source() {
        return data_source;
    }

    public String getOwner() {
        return owner;
    }

    public String getObject_name() {
        return object_name;
    }

    public void setData_source(Data_Source data_source) {
        this.data_source = data_source;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setObject_name(String object_name) {
        this.object_name = object_name;
    }

    public static class Data_Source {
        private Server server;
        private String instance_name;
        private String ds_name;

        public Server getServer() {
            return server;
        }

        public String getInstance_name() {
            return instance_name;
        }

        public String getDs_name() {
            return ds_name;
        }

        public void setServer(Server server) {
            this.server = server;
        }

        public void setInstance_name(String instance_name) {
            this.instance_name = instance_name;
        }

        public void setDs_name(String ds_name) {
            this.ds_name = ds_name;
        }

        public static class Server {
            private String host;
            private String port;

            public String getHost() {
                return host;
            }

            public String getPort() {
                return port;
            }

            public void setHost(String host) {
                this.host = host;
            }

            public void setPort(String port) {
                this.port = port;
            }
        }
    }
}
