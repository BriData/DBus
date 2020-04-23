package com.creditease.dbus.router.spout.handler.processor;

import java.util.function.Supplier;

public interface Processor {

    Object process(Object obj, Supplier... suppliers);

}
