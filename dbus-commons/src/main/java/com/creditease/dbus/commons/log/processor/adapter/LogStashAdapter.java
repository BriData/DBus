package com.creditease.dbus.commons.log.processor.adapter;

import java.util.*;


public class LogStashAdapter implements Iterator<String> {

    private String value;

    private Iterator<String> it;

    public LogStashAdapter(String value) {
        this.value = value;
        adapt();
    }

    private void adapt() {
        List<String> wk = new ArrayList<>();
        wk.add(value);
        it = wk.iterator();
    }

    public boolean hasNext() {
        return it.hasNext();
    }

    public String next() {
        return it.next();
    }

}
