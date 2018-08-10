package com.creditease.dbus.log.resource.local;


import com.creditease.dbus.log.resource.AbstractConfigResource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public abstract class FileConfigResource<T> extends AbstractConfigResource<T> {

    protected FileConfigResource(String name) {
        super(name);
    }

    protected void init() {
        FileInputStream fis = null;
        try {
            prop = new Properties();
            File file = new File(SystemUtils.USER_DIR.replaceAll("\\\\", "/") + "/conf/" + name);
            fis = new FileInputStream(file);
            prop.load(fis);
        } catch (IOException e) {
            throw new RuntimeException("inti config resource " + name + " error!");
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

}
