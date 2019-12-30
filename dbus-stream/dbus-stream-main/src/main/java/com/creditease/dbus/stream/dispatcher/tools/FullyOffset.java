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


package com.creditease.dbus.stream.dispatcher.tools;

import java.io.Serializable;

public class FullyOffset implements Serializable {
    private long kafkaOffset;
    private int partitionOffset;
    private int subOffset;

    public FullyOffset(long kafkaOffset, int partitionOffset, int subOffset) {
        this.kafkaOffset = kafkaOffset;
        this.partitionOffset = partitionOffset;
        this.subOffset = subOffset;
    }

    private int compareKafkaOffset(FullyOffset other) {
        if (this.kafkaOffset > other.getKafkaOffset()) {
            return 1;
        } else if (this.kafkaOffset == other.getKafkaOffset()) {
            return 0;
        } else {
            return -1;
        }
    }

    private int comparePartID(FullyOffset other) {
        if (this.partitionOffset > other.getPartitionOffset()) {
            return 1;
        } else if (this.partitionOffset == other.getPartitionOffset()) {
            return 0;
        } else {
            return -1;
        }
    }

    private int compareSubID(FullyOffset other) {
        if (this.subOffset > other.getSubOffset()) {
            return 1;
        } else if (this.subOffset == other.getSubOffset()) {
            return 0;
        } else {
            return -1;
        }
    }

    public int compare(FullyOffset other) {
        int ret = compareKafkaOffset(other);
        if (ret != 0) {
            return ret;
        }
        ret = comparePartID(other);
        if (ret != 0) {
            return ret;
        }

        return compareSubID(other);
    }

    /**
     * isZeroOffset
     * Check if the kafkaOffset is zero kafkaOffset
     *
     * @return true/false
     */
    public boolean isZeroOffset() {
        return (this.kafkaOffset == 0 && this.partitionOffset == 0 && this.subOffset == 0);
    }

    public String toString() {
        return String.format("%d-%d-%d", kafkaOffset, partitionOffset, subOffset);
    }

    public int getPartitionOffset() {
        return partitionOffset;
    }

    public void setPartitionOffset(int partitionOffset) {
        this.partitionOffset = partitionOffset;
    }

    public long getKafkaOffset() {
        return kafkaOffset;
    }

    public void setKafkaOffset(long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }

    public int getSubOffset() {
        return subOffset;
    }

    public void setSubOffset(int subOffset) {
        this.subOffset = subOffset;
    }
}
