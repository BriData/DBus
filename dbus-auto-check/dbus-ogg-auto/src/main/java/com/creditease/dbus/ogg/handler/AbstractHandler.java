package com.creditease.dbus.ogg.handler;

import java.io.BufferedWriter;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public abstract class AbstractHandler implements IHandler{

    public boolean process(BufferedWriter bw) {
        boolean isOk = true;
        try {
            checkDeploy(bw);
        } catch (Exception e) {
            isOk = false;
        }
        return isOk;
    }

    public abstract void checkDeploy(BufferedWriter bw) throws Exception;
}
