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


package com.creditease.dbus.utils;

import com.creditease.dbus.encoders.Encoder;
import org.reflections.Reflections;

import java.lang.annotation.Annotation;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by zhangyf on 17/8/16.
 */
public class AnnotationScanner {

    public static Set<Class<?>> scan(String rootPackage, Class<? extends Annotation> annotation) {
        Reflections reflections = new Reflections(rootPackage);
        return reflections.getTypesAnnotatedWith(annotation);
    }

    public static Set<Class<?>> scan(Class<? extends Annotation> annotation) {
        return AnnotationScanner.scan(null, annotation);
    }

    public static Set<Class<?>> scan(String rootPackage, Class<? extends Annotation> annotation, Filter filter) {
        Set<Class<?>> classes = scan(rootPackage, annotation);
        Iterator<Class<?>> it = classes.iterator();
        while (it.hasNext()) {
            if (!filter.filter(it.next(), annotation)) {
                it.remove();
            }
        }
        return classes;
    }

    public static interface Filter {
        boolean filter(Class<?> clazz, Class<? extends Annotation> annotation);
    }

    public static void main(String[] args) {
        Set set = AnnotationScanner.scan(null, Encoder.class);
        System.out.println(set);
    }
}
