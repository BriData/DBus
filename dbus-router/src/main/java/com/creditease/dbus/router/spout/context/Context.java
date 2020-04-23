package com.creditease.dbus.router.spout.context;

import com.creditease.dbus.router.base.DBusRouterBase;

public class Context {

    protected DBusRouterBase inner;

    public Context(DBusRouterBase inner) {
        this.inner = inner;
    }

    public DBusRouterBase getInner() {
        return inner;
    }
}
