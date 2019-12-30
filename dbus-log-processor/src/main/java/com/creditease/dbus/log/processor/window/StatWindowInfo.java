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


package com.creditease.dbus.log.processor.window;


public class StatWindowInfo extends Element {

    private Long successCnt = 0L;

    private Long errorCnt = 0L;

    private Long readKafkaCount = 0L;

    public Long getSuccessCnt() {
        return successCnt;
    }

    public void setSuccessCnt(Long successCnt) {
        this.successCnt = successCnt;
    }

    public Long getErrorCnt() {
        return errorCnt;
    }

    public void setErrorCnt(Long errorCnt) {
        this.errorCnt = errorCnt;
    }

    public Long getReadKafkaCount() {
        return readKafkaCount;
    }

    public void setReadKafkaCount(Long readKafkaCount) {
        this.readKafkaCount = readKafkaCount;
    }


    @Override
    public void merge(Element e, Integer taskIdSum) {
        super.merge(e, taskIdSum);
        this.successCnt += ((StatWindowInfo) e).getSuccessCnt();
        this.errorCnt += ((StatWindowInfo) e).getErrorCnt();
        this.readKafkaCount += ((StatWindowInfo) e).getReadKafkaCount();
    }
}
