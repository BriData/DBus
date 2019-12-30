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


package com.creditease.dbus.common.format;

import com.creditease.dbus.common.FullPullConstants;

import java.sql.Types;


public class DataDBInputSplit extends InputSplit {
    private int sqlType;
    private String splitCol;
    private String lowerOperator;
    private Object lowerValue;
    private String upperOperator;
    private Object upperValue;

    /**
     * Default Constructor.
     */
    public DataDBInputSplit() {
    }

    /**
     * Convenience Constructor.
     */
    public DataDBInputSplit(int sqlType, final String splitCol, final String lowerOperator,
                            final Object lowerValue, final String upperOperator, final Object upperValue) {
        this.sqlType = sqlType;
        this.splitCol = splitCol;
        this.lowerOperator = lowerOperator;
        this.lowerValue = lowerValue;
        this.upperOperator = upperOperator;
        this.upperValue = upperValue;
    }

    public String getCondWithPlaceholder() {
        String collate = "";
        if (Types.VARCHAR == this.sqlType) {
            collate = super.getCollate();
        }
        StringBuffer sb = new StringBuffer();
        sb.append(this.splitCol).append(collate).append(this.lowerOperator);
        if (!FullPullConstants.QUERY_COND_IS_NULL.equals(this.lowerOperator)) {
            sb.append("?");
        }
        sb.append(" AND ").append(this.splitCol).append(collate).append(this.upperOperator);
        if (!FullPullConstants.QUERY_COND_IS_NULL.equals(this.upperOperator)) {
            sb.append("?");
        }
        return sb.toString();
    }

    public String getSplitCol() {
        return splitCol;
    }

    public String getLowerOperator() {
        return lowerOperator;
    }

    public Object getLowerValue() {
        return lowerValue;
    }

    public String getUpperOperator() {
        return upperOperator;
    }

    public Object getUpperValue() {
        return upperValue;
    }

    public void setSplitCol(String splitCol) {
        this.splitCol = splitCol;
    }

    public void setLowerOperator(String lowerOperator) {
        this.lowerOperator = lowerOperator;
    }

    public void setLowerValue(Object lowerValue) {
        this.lowerValue = lowerValue;
    }

    public void setUpperOperator(String upperOperator) {
        this.upperOperator = upperOperator;
    }

    public void setUpperValue(Object upperValue) {
        this.upperValue = upperValue;
    }

    public int getSqlType() {
        return sqlType;
    }

    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }
}
