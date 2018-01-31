package com.creditease.dbus.commons.log.processor.parse;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/10/12.
 */
public class Field implements Serializable {

    private Object value;
    private String name;
    private String type;
    private Boolean nullable;
    private Boolean encoded;

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public Boolean getEncoded() {
        return encoded;
    }

    public void setEncoded(Boolean encoded) {
        this.encoded = encoded;
    }
}
