package com.creditease.dbus.stream.dispatcher.tools;

/**
 * Created by zhenlinzhong on 2018/4/25.
 */
public interface OffsetResetProvider {
    void offsetReset(Object... args);
}
