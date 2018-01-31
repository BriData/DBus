package com.creditease.dbus.log.processor.window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LogProcessorWindow {

    private static Logger logger = LoggerFactory.getLogger(LogProcessorWindow.class);

    private int width;

    private Map<String, Element> cache;

    private Integer taskIdSum;

    public LogProcessorWindow(int width, Integer taskIdSum) {
        this.width = width;
        this.taskIdSum = taskIdSum;
        cache = new HashMap<>();
    }

    public void offer(Element e) {
        if (cache.containsKey(e.getKey())) {
            cache.get(e.getKey()).merge(e, taskIdSum);
        } else {
            e.merge(taskIdSum);
            cache.put(e.getKey(), e);
        }
    }

    public List<Element> deliver() {
        List<String> okKeys = new ArrayList<>();
        List<Element> okValues = new ArrayList<>();
        for(Map.Entry<String, Element> entry : cache.entrySet()) {
            if(entry.getValue().getOk()) {
                okKeys.add(entry.getKey());
                okValues.add(entry.getValue());
            }
        }
        for (String key : okKeys) {
            cache.remove(key);
        }
        if(cache.size() > width) {
            logger.error("cache.size: {}, cache overflow!", cache.size());
        }
        return okValues;
    }

}
