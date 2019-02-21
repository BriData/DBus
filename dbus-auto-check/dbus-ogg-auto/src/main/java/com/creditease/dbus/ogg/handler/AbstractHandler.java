package com.creditease.dbus.ogg.handler;

import java.io.BufferedWriter;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public abstract class AbstractHandler implements IHandler{

    public void process(BufferedWriter bw) throws Exception{
        try {
            checkDeploy(bw);
        } catch (Exception e) {
            throw e;
        }
    }

    public abstract void checkDeploy(BufferedWriter bw) throws Exception;
}
