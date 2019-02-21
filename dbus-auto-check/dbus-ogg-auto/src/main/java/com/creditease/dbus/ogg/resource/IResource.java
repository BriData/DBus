package com.creditease.dbus.ogg.resource;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public interface IResource<T> {

    T load() throws Exception;

}
