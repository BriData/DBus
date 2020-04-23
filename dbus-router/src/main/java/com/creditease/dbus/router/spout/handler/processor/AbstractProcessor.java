package com.creditease.dbus.router.spout.handler.processor;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractProcessor implements Processor {

    private AtomicBoolean isBreak = new AtomicBoolean(false);

    protected boolean isBreak() {
        return isBreak.get();
    }

    protected abstract boolean isBelong(String str);

}
