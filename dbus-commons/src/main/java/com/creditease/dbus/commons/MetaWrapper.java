/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.commons;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

import static com.creditease.dbus.commons.SupportedOraDataType.NUMBER;
import static com.creditease.dbus.commons.SupportedOraDataType.isSupported;

/**
 * Meta信息包装器
 * Created by Shrimp on 16/5/17.
 */
public class MetaWrapper implements Serializable {
    private static Logger LOG = LoggerFactory.getLogger(MetaWrapper.class);
    /**
     * meta 索引,用来快速定位meta在metaCells中的位置
     */
    private Map<String, Integer> index;
    private List<MetaCell> metaCells;

    public MetaWrapper() {
        metaCells = new ArrayList<>();
        index = new HashMap<>();
    }

    public void sortMetaCells() {
        metaCells.sort(new Comparator<MetaCell>() {
            @Override
            public int compare(MetaCell o1, MetaCell o2) {
                return o1.getInternalColumnId() - o2.getInternalColumnId();
            }
        });
        index.clear();
        for (int i = 0; i < metaCells.size(); i++) {
            index.put(metaCells.get(i).getColumnName(), i);
        }
    }

    public boolean contains(String tableName) {
        return index.containsKey(tableName);
    }

    public List<MetaCell> filter() {
        List<MetaCell> list = new ArrayList<>();
        for (MetaCell cell : metaCells) {
            if (cell.isSupported()) {
                list.add(cell);
            }
        }
        LOG.info("[MetaWrapper] Meta size before/after filter is: {} vs {}.", metaCells.size(), list.size());
        return list;
    }

    public List<MetaCell> filterOnMysql() {
        List<MetaCell> list = new ArrayList<>();
        for (MetaCell cell : metaCells) {
            if (cell.isSupportedOnMysql()) {
                list.add(cell);
            }
        }
        return list;
    }

    public boolean isEmpty() {
        return metaCells.isEmpty();
    }

    public void addMetaCell(MetaCell cell) {
        metaCells.add(cell);
        index.put(cell.getColumnName(), metaCells.size() - 1);
    }

    public List<MetaCell> getColumns() {
        return metaCells;
    }

    public MetaCell getIfSupport(String columnName) {
        if (index.containsKey(columnName)) {
            MetaCell cell = get(columnName);
            if (isSupported(cell.getDataType())) {
                return cell;
            } else {
                return null;
            }
        }
        return null;
    }

    public MetaCell get(String columnName) {
        if (index.containsKey(columnName)) {
            Integer idx = index.get(columnName);
            return metaCells.get(idx);
        }
        return null;
    }

    public Map<String, Integer> getIndex() {
        return index;
    }

    public void setIndex(Map<String, Integer> index) {
        this.index = index;
    }

    public List<MetaCell> getMetaCells() {
        return metaCells;
    }

    public void setMetaCells(List<MetaCell> metaCells) {
        this.metaCells = metaCells;
    }

    @Override
    public String toString() {
        return toJson();
    }

    public String toJson() {
        try {
            return JSON.toJSONString(this);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    /**
     * 比较两个版本的meta是否兼容(需要过滤掉dbus系统无法支持的类型)
     * 不兼容的情况:
     * 1 字段数不一致
     * 2 字段类型不一致
     * 3 number类型的字段小数位由0变为非0或由非0变为0
     */
    public static boolean isCompatible(MetaWrapper originalMeta, MetaWrapper dbusMeta) {
        // 获取过滤之后的meta信息
        LOG.info("[MetaWrapper]  Cached meta filtering......");
        List<MetaCell> originalMetaCells = originalMeta.filter();

        // 管理库中的meta信息
        LOG.info("[MetaWrapper]  Meta filtering......");
        List<MetaCell> dbusMetaCells = dbusMeta.filter();

        // 如果过滤之后的字段数不相同则返回false
        if (originalMetaCells.size() != dbusMetaCells.size()) {
            LOG.warn("过滤之后的字段数不相同, 源库{} vs dbus管理库{},导致 Meta不兼容。", originalMetaCells.size(), dbusMetaCells.size());
            return false;
        }

        LOG.info("过滤之后的字段数相同。");

        // 如果两个集合中存在不同名字的字段则返回false
        for (MetaWrapper.MetaCell originalCell : originalMetaCells) {
            if (!dbusMeta.contains(originalCell.getColumnName())) {
                LOG.error("两个集合中存在不同名字的字段{}，Meta不兼容。", originalCell.getColumnName());
                return false;
                //比较每个字段信息
            } else if (!compareCell(originalCell, dbusMeta.get(originalCell.getColumnName()))) {
                LOG.error("Meta Cell compare failed on Column {}", originalCell.getColumnName());
                return false;
            }
        }
        return true;
    }

    public boolean isCompatibleOnMysql(MetaWrapper meta) {
        MetaWrapper cachedMeta = this;
        List<MetaCell> metaCells0 = cachedMeta.filterOnMysql();
        List<MetaCell> metaCells1 = meta.filterOnMysql();
        if (metaCells0.size() != metaCells1.size()) {
            return false;
        }

        // 如果两个集合中存在不同名字的字段则返回false
        for (MetaWrapper.MetaCell cell : metaCells0) {
            if (!meta.contains(cell.getColumnName())) {
                return false;
            } else if (!compareCellOnMysql(cell, meta.get(cell.getColumnName()))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 判断外部版本号是否需要发生变化
     */
    private static boolean compareCell(MetaWrapper.MetaCell originalCell, MetaWrapper.MetaCell dbusCell) {
        boolean result = true;
        // 判断数据类型是否相同
        if (!originalCell.getDataType().equalsIgnoreCase(dbusCell.getDataType())) {
            // 判断meta是否均为字符类型
            if (isCharacterType(originalCell.getDataType()) && isCharacterType(dbusCell.getDataType())) {
                return true;
            }
            LOG.error("Column {}类型不同。 源库{} vs dbus管理库{}.", originalCell.getColumnName(),
                    dbusCell.getDataType(), originalCell.getDataType());
            result = false;
        } else if (dbusCell.getDataType().equals(NUMBER.name())
                && (originalCell.getDataScale() ^ dbusCell.getDataScale()) != 0) {
            // 如果小数位不同
            if (originalCell.getDataScale() * dbusCell.getDataScale() == 0) { // 是否存在零的情况
                LOG.error("Column {}类型为{}。DataScale 不兼容。源库{} vs dbus管理库{} ",
                        dbusCell.getColumnName(), dbusCell.getDataType(),
                        originalCell.getDataScale(), dbusCell.getDataScale());
                result = false;
            }
        }
        return result;
    }

    private boolean compareCellOnMysql(MetaWrapper.MetaCell cell0, MetaWrapper.MetaCell cell) {
        boolean result = true;
        // 判断数据类型是否相同
        if (!cell0.getDataType().equalsIgnoreCase(cell.getDataType())) {
            // 判断meta是否均为字符类型
            if (isCharacterTypeOnMysql(cell0.getDataType()) && isCharacterType(cell.getDataType())) {
                return true;
            }
            result = false;
        }
        return result;
    }

    private static boolean isCharacterType(String type) {
        return SupportedOraDataType.isCharacterType(type);
    }

    private boolean isCharacterTypeOnMysql(String type) {
        return SupportedMysqlDataType.isCharacterType(type);
    }

    /**
     * meta info
     */
    public static class MetaCell implements Cloneable, Serializable {
        private String owner;
        private String tableName;
        private String columnName;
        private String originalColumnName;
        private int columnId;
        private int version;
        private String dataType;
        private Long dataLength;
        private Integer dataPrecision;
        private Integer dataScale;
        private String nullAble;
        private String isPk;
        private int pkPosition;
        private Timestamp ddlTime;
        private int charLength;
        private int internalColumnId;
        private String hiddenColumn;
        private String virtualColumn;
        private String comments;

        //TODO 目前只有oracle使用
        private String defaultValue;

        public String getComments() {
            return comments;
        }

        public void setComments(String comments) {
            this.comments = comments;
        }

        /** 对于oracle来讲可以为B/C，数据类型char(1 char)类型为C, char(1)和char(1 byte) 为B，默认为B */
        private String charUsed;

        public boolean isSupported() {
            return SupportedOraDataType.isSupported(getDataType()) && !isHidden() && !isVirtual();
        }
        public boolean isHidden() {
            return hiddenColumn != null && "YES".equals(hiddenColumn);
        }

        public boolean isVirtual() {
            return virtualColumn != null && "YES".equals(virtualColumn);
        }

        public boolean isSupportedOnMysql() {
            return SupportedMysqlDataType.isSupported(getDataType());
        }

        public boolean isNullable() {
            return "Y".equals(getNullAble());
        }

        public String getOwner() {
            return owner;
        }

        public void setOwner(String owner) {
            this.owner = owner;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getOriginalColumnName() {
            return originalColumnName;
        }

        public void setOriginalColumnName(String originalColumnName) {
            this.originalColumnName = originalColumnName;
        }

        public int getColumnId() {
            return columnId;
        }

        public void setColumnId(int columnId) {
            this.columnId = columnId;
        }

        public Integer getVersion() {
            return version;
        }

        public void setVersion(Integer version) {
            this.version = version;
        }

        public String getDataType() {
            return dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }

        public Long getDataLength() {
            return dataLength;
        }

        public void setDataLength(Long dataLength) {
            this.dataLength = dataLength;
        }

        public Integer getDataPrecision() {
            return dataPrecision;
        }

        public void setDataPrecision(Integer dataPrecision) {
            this.dataPrecision = dataPrecision;
        }

        public Integer getDataScale() {
            return dataScale;
        }

        public void setDataScale(int dataScale) {
            this.dataScale = dataScale;
        }

        public String getNullAble() {
            return nullAble;
        }

        public void setNullAble(String nullAble) {
            this.nullAble = nullAble;
        }

        public String getIspk() {
            return isPk;
        }

        public void setIsPk(String isPk) {
            this.isPk = isPk;
        }

        public Integer getPkPosition() {
            return pkPosition;
        }

        public void setPkPosition(Integer pkPosition) {
            this.pkPosition = pkPosition;
        }

        public Timestamp getDdlTime() {
            return ddlTime;
        }

        public void setDdlTime(Timestamp ddlTime) {
            this.ddlTime = ddlTime;
        }

        public void setCharLength(int charLength) {
            this.charLength = charLength;
        }

        public int getCharLength() {
            return charLength;
        }

        public void setCharUsed(String charUsed) {
            this.charUsed = charUsed;
        }

        public String getCharUsed() {
            return charUsed;
        }

        public int getInternalColumnId() {
            return internalColumnId;
        }

        public void setInternalColumnId(int internalColumnId) {
            this.internalColumnId = internalColumnId;
        }

        public String getHiddenColumn() {
            return hiddenColumn;
        }

        public void setHiddenColumn(String hiddenColumn) {
            this.hiddenColumn = hiddenColumn;
        }

        public String getVirtualColumn() {
            return virtualColumn;
        }

        public void setVirtualColumn(String virtualColumn) {
            this.virtualColumn = virtualColumn;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        //        public Object getDefaultValue() {
//            return defaultValue;
//        }
//
//        public void setDefaultValue(Object defaultValue) {
//            this.defaultValue = defaultValue;
//        }
//
//        public String getDefaultValueStr() {
//            // mysql不考虑默认值问题
//            if (getDefaultValue() == null || !SupportedOraDataType.isSupported(getDataType())) return null;
//            SupportedOraDataType type = parse(getDataType());
//            switch (type) {
//                case DATE:
//                case TIMESTAMP:
//                    return DateFormatUtils.format((Date) getDefaultValue(), "yyyy-MM-dd HH:mm:ss.SSS");
//                default:
//                    return getDefaultValue().toString();
//            }
//        }
//
//        public static Object parseDefaultValue(String dataType, String data) {
//            // mysql不考虑默认值问题
//            if (data == null || !SupportedOraDataType.isSupported(dataType)) return null;
//            SupportedOraDataType type = parse(dataType);
//            switch (type) {
//                case DATE:
//                case TIMESTAMP:
//                    try {
//                        return DateUtils.parseDate(data, "yyyy-MM-dd HH:mm:ss.SSS");
//                    } catch (ParseException e) {
//                        throw new IllegalArgumentException("Date or timestamp string of default value can't match with pattern:" + "yyyy-MM-dd HH:mm:ss.SSS");
//                    }
//                default:
//                    return data;
//            }
//        }
    }
}
