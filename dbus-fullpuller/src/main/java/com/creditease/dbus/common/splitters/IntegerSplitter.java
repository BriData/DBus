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


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creditease.dbus.common.splitters;

import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.format.DataDBInputSplit;
import com.creditease.dbus.common.format.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Fix bug by DBus Team 20161230
 * Implement DBSplitter over integer values.
 */
public class IntegerSplitter implements DBSplitter {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final int type = Types.BIGINT;

    public List<InputSplit> split(long numSplits, ResultSet results, String colName, DBConfiguration dbConf) throws SQLException {

        long minVal = results.getLong(1);
        long maxVal = results.getLong(2);

        if (numSplits < 1L) {
            numSplits = 1L;
        }

        if (results.getString(1) == null && results.getString(2) == null) {
            // Range is null to null. Return a null split accordingly.
            List<InputSplit> splits = new ArrayList<InputSplit>();
            splits.add(new DataDBInputSplit(type, colName, FullPullConstants.QUERY_COND_IS_NULL, null, FullPullConstants.QUERY_COND_IS_NULL, null));
            return splits;
        }

        long splitLimit = dbConf.getSplitShardSize();

        // Get all the split points together.
        List<Long> splitPoints = split(numSplits, splitLimit, minVal, maxVal);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Splits: [%,28d to %,28d] into %d parts",
                    minVal, maxVal, numSplits));
            for (int i = 0; i < splitPoints.size(); i++) {
                logger.debug(String.format("%,28d", splitPoints.get(i)));
            }
        }
        List<InputSplit> splits = new ArrayList<InputSplit>();

        // Turn the split points into a set of intervals.
        long start = splitPoints.get(0);
        for (int i = 1; i < splitPoints.size(); i++) {
            long end = splitPoints.get(i);

            if (i == splitPoints.size() - 1) {
                // This is the last one; use a closed interval.
                splits.add(new DataDBInputSplit(type, colName, " >= ", start, " <= ", end));
            } else {
                // Normal open-interval case.
                splits.add(new DataDBInputSplit(type, colName, " >= ", start, " < ", end));
            }

            start = end;
        }

        if (results.getString(1) == null || results.getString(2) == null) {
            // At least one extrema is null; add a null split.
            splits.add(new DataDBInputSplit(type, colName, FullPullConstants.QUERY_COND_IS_NULL, null, FullPullConstants.QUERY_COND_IS_NULL, null));
        }

        return splits;
    }

    /**
     * Returns a list of longs one element longer than the list of input splits.
     * This represents the boundaries between input splits.
     * All splits are open on the top end, except the last one.
     * <p>
     * So the list [0, 5, 8, 12, 18] would represent splits capturing the
     * intervals:
     * <p>
     * [0, 5)
     * [5, 8)
     * [8, 12)
     * [12, 18] note the closed interval for the last split.
     *
     * @param numSplits  Number of split chunks.
     * @param splitLimit Limit the split size.
     * @param minVal     Minimum value of the set to split.
     * @param maxVal     Maximum value of the set to split.
     * @return Split values inside the set.
     * @throws SQLException In case of SQL exception.
     */
    public List<Long> split(long numSplits, long splitLimit, long minVal, long maxVal)
            throws SQLException {

        List<Long> splits = new ArrayList<Long>();

        // We take the min-max interval and divide by the numSplits and also
        // calculate a remainder.  Because of integer division rules, numsplits *
        // splitSize + minVal will always be <= maxVal.  We then use the remainder
        // and add 1 if the current split index is less than the < the remainder.
        // This is guaranteed to add up to remainder and not surpass the value.


        long splitSize = (maxVal - minVal) / numSplits;
//      double splitSizeDouble = ((double)maxVal - (double)minVal) / (double)numSplits;
//
//      if (splitLimit > 0 && splitSizeDouble > splitLimit) {
//        // If split size is greater than limit then do the same thing with larger
//        // amount of splits.
//         logger.debug("Adjusting split size " + splitSize
//          + " because it's greater than limit " + splitLimit);
//        long newSplits = (maxVal - minVal) / splitLimit;
//        return split(newSplits != numSplits ? newSplits : newSplits + 1,
//         splitLimit, minVal, maxVal);
//      }
//      logger.info("Split size: " + splitSize + "; Num splits: " + numSplits
//       + " from: " + minVal + " to: " + maxVal);

        long remainder = (maxVal - minVal) % numSplits;
        long curVal = minVal;

        // This will honor numSplits as long as split size > 0.  If split size is
        // 0, it will have remainder splits.
        for (int i = 0; i <= numSplits; i++) {
            splits.add(curVal);
            if (curVal >= maxVal) {
                break;
            }
            curVal += splitSize;
            curVal += (i < remainder) ? 1 : 0;
        }

        if (splits.size() == 1) {
            // make a valid singleton split
            splits.add(maxVal);
        } else if ((maxVal - minVal) <= numSplits) {
            // Edge case when there is lesser split points (intervals) then
            // requested number of splits. In such case we are creating last split
            // with two values, for example interval [1, 5] broken down into 5
            // splits will create following conditions:
            //  * 1 <= x < 2
            //  * 2 <= x < 3
            //  * 3 <= x < 4
            //  * 4 <= x <= 5
            // Notice that the last split have twice more data than others. In
            // those cases we add one maxVal at the end to create following splits
            // instead:
            //  * 1 <= x < 2
            //  * 2 <= x < 3
            //  * 3 <= x < 4
            //  * 4 <= x < 5
            //  * 5 <= x <= 5
            splits.add(maxVal);
        }

        return splits;
    }
}
