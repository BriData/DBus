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


package com.creditease.dbus.extractor.container;

import com.creditease.dbus.extractor.vo.ExtractorVo;
import com.creditease.dbus.extractor.vo.JdbcVo;
import com.creditease.dbus.extractor.vo.OutputTopicVo;

import java.util.List;
import java.util.Properties;
import java.util.Set;

public class ExtractorConfigContainer {

    private static ExtractorConfigContainer container;

    private List<JdbcVo> jdbc;

    private ExtractorVo extractorConfig;

    private Properties kafkaProducerConfig;

    private Properties kafkaConsumerConfig;

    private Set<OutputTopicVo> outputTopic;
    //private OutputTopicVo controlTopic;

    private String filter;

    private ExtractorConfigContainer() {
    }

    public static ExtractorConfigContainer getInstances() {
        if (container == null) {
            synchronized (ExtractorConfigContainer.class) {
                if (container == null)
                    container = new ExtractorConfigContainer();
            }
        }
        return container;
    }

    public List<JdbcVo> getJdbc() {
        return jdbc;
    }

    public void setJdbc(List<JdbcVo> jdbc) {
        this.jdbc = jdbc;
    }

    public ExtractorVo getExtractorConfig() {
        return extractorConfig;
    }

    public void setExtractorConfig(ExtractorVo extractorConfig) {
        this.extractorConfig = extractorConfig;
    }

    public Properties getKafkaProducerConfig() {
        return kafkaProducerConfig;
    }

    public void setKafkaProducerConfig(Properties kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    public Properties getKafkaConsumerConfig() {
        return kafkaConsumerConfig;
    }

    public void setKafkaConsumerConfig(Properties kafkaConsumerConfig) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
    }

    public Set<OutputTopicVo> getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(Set<OutputTopicVo> outputTopic) {
        this.outputTopic = outputTopic;
    }

    /*
        public OutputTopicVo getControlTopic() {
            return controlTopic;
        }

        public void setControlTopic(OutputTopicVo controlTopic) {
            this.controlTopic = controlTopic;
        }
    */
    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public void clear() {
        jdbc = null;
        extractorConfig = null;
        kafkaProducerConfig = null;
        kafkaConsumerConfig = null;
        outputTopic = null;
    }
}
