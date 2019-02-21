package com.creditease.dbus.ogg.handler;

import java.io.BufferedWriter;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public interface IHandler {
    void process(BufferedWriter bw) throws Exception;
}
