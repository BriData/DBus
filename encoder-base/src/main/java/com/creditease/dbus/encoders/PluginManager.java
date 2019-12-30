/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package com.creditease.dbus.encoders;

import com.google.common.base.Joiner;
import org.apache.commons.io.IOUtils;
import org.reflections.Configuration;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.annotation.Inherited;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhangyf on 2018/4/27.
 */
public class PluginManager {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private static final String PROTOCOL = "file";
    private PluginLoader loader;
    private volatile Map<String, ExtEncodeStrategy> encoderMap;
    private Class<? extends Annotation> annotation;
    private Lock reloadLock;

    public PluginManager(Class<? extends Annotation> clazz, PluginLoader loader) throws Exception {
        this.loader = loader;
        this.annotation = clazz;
        this.reloadLock = new ReentrantLock();
        this.initialize();
    }

    public void reload() throws Exception {
        if (this.reloadLock.tryLock()) {
            try {
                logger.info("plugin manager is beginning to reload.");
                initialize();
            } finally {
                this.reloadLock.unlock();
            }
        } else {
            logger.warn("PluginManager is reloading");
        }
    }

    public ExtEncodeStrategy getPlugin(String encoderId, String type) {
        return encoderMap.get(pluginKey(encoderId, type));
    }

    /**
     * copy on write 机制实现重新加载
     */
    private void initialize() throws Exception {
        List<EncodePlugin> plugins = loader.loadPlugins();
        Set<String> urls = new HashSet<>();
        Map<String, ExtEncodeStrategy> map = new ConcurrentHashMap<>();
        for (EncodePlugin plugin : plugins) {
            if (!urls.contains(plugin.getJarPath())) {
                URL jar = new URL(PROTOCOL, null, plugin.getJarPath());
                PluginScanner scanner = new ReflectionsPluginScanner(ConfigurationBuilder.build(jar));
                Iterable<String> classes = scanner.scan(this.annotation);
                buildEncoders(classes, jar, plugin.getId(), map);
                urls.add(plugin.getJarPath());
            }
        }
        this.encoderMap = map;
    }

    private void buildEncoders(Iterable<String> encoders, URL jar,
                               String pluginId, Map<String, ExtEncodeStrategy> encoderMap) throws Exception {
        if (encoders.iterator().hasNext()) {
            PluginsClassloader classloader = new PluginsClassloader(PluginManager.class.getClassLoader());
            classloader.addJar(jar);
            for (String encoderClass : encoders) {
                Class<?> clazz = classloader.loadClass(encoderClass);
                ExtEncodeStrategy encoder = (ExtEncodeStrategy) clazz.newInstance();
                Encoder e = clazz.getDeclaredAnnotation(Encoder.class);
                String[] resources = e.resources();
                for (String resource : resources) {
                    InputStream is = null;
                    try {
                        is = classloader.getResourceAsStream(resource);
                        if (is == null) {
                            throw new FileNotFoundException("resource " + resource + " not found in " + jar);
                        }
                        encoder.addResource(resource, IOUtils.toByteArray(is));
                    } finally {
                        IOUtils.closeQuietly(is);
                    }
                }
                encoderMap.put(pluginKey(pluginId, e.type()), encoder);
                logger.info("ExtEncodeStrategy:{} pluginId:{} was built from {}", encoderClass, pluginId, jar.toString());
            }
        }
    }

    private static String pluginKey(String id, String type) {
        return Joiner.on("_").join(id, type);
    }

    public static void main(String[] args) throws Exception {
        PluginManager m = new PluginManager(Encoder.class, () -> {
            List<EncodePlugin> plugins = new LinkedList<>();
            EncodePlugin plugin = new EncodePlugin();
            plugin.setId("1");
            plugin.setName("address");
            plugin.setJarPath("encoder-plugins\\target\\encoder-plugins-0.4.0.jar");
            plugins.add(plugin);

            return plugins;
        });
        ExtEncodeStrategy address = m.getPlugin("1", "address");

        System.out.println(m.encoderMap);
    }
}

class ReflectionsPluginScanner extends Reflections implements PluginScanner {
    public ReflectionsPluginScanner(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Iterable<String> scan(final Class<? extends Annotation> annotation) {
        Iterable<String> annotated = store.get(TypeAnnotationsScanner.class.getSimpleName(), annotation.getName());
        return getAllAnnotated(annotated, annotation.isAnnotationPresent(Inherited.class), false);
    }
}

class PluginsClassloader extends URLClassLoader {
    public PluginsClassloader(ClassLoader parent) {
        super(new URL[0], parent);
    }

    public void addJar(URL jar) throws Exception {
        this.addURL(jar);
    }
}
