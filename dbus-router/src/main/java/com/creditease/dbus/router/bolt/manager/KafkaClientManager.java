package com.creditease.dbus.router.bolt.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import com.creditease.dbus.commons.Pair;
import com.creditease.dbus.router.cache.Cache;
import com.creditease.dbus.router.cache.impl.PerpetualCache;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaClientManager<T> {

    private static Logger logger = LoggerFactory.getLogger(KafkaClientManager.class);

    protected String securityConf = null;
    protected Properties properties = null;
    protected Cache urlCache = null;
    protected Cache kafkaClientCache = null;
    private static final String SPLITTER = ",";

    public KafkaClientManager(Properties properties, String securityConf) {
        this.properties = properties;
        this.securityConf = securityConf;
        urlCache = new PerpetualCache("url-cache");
        kafkaClientCache = new PerpetualCache("kafka-client-cache");
    }

    public Pair getKafkaClient(String url) {
        Object client = kafkaClientCache.getObject(url);
        if (client != null)
            return new Pair(url, (T) client);

        Pair<String, T> ret = new Pair(null, null);
        List<String> urls = new ArrayList<>(Arrays.asList(StringUtils.split(url, SPLITTER)));
        AtomicBoolean isExist = new AtomicBoolean(false);

        urlCache.foreach((key, value) -> {
            String strKey = (String) key;
            List<String> listValue = (List<String>) value;
            listValue.retainAll(urls);
            urlCache.putObject(strKey, new ArrayList<>(Arrays.asList(StringUtils.split(strKey, SPLITTER))));
            if (!listValue.isEmpty()) {
                isExist.set(true);
                ret.setKey(strKey);
                ret.setValue((T) kafkaClientCache.getObject(key));
                return true;
            }
            return false;
        });
        if (!isExist.get()) {
            urlCache.putObject(url, urls);
            kafkaClientCache.putObject(url, obtainKafkaClient(url));
            ret.setKey(url);
            ret.setValue((T) kafkaClientCache.getObject(url));
        }
        return ret;
    }

    protected void close() {
        urlCache.clear();
    }

    protected abstract T obtainKafkaClient(String url);

}
