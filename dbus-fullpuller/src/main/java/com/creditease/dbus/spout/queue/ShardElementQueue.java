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


package com.creditease.dbus.spout.queue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShardElementQueue {
    private List<ShardElement> queue;

    public ShardElementQueue() {
        queue = new ArrayList<>();
    }

    public void add(long offset, long index) {
        ShardElement elem = getShardElement(offset);
        if (elem != null) {
            elem.emit();
        } else {
            queue.add(new ShardElement(offset, index));
        }
    }

    public ShardElement seekPoint() {
        for (ShardElement e : queue) {
            if (e.isFailed()) return e;
        }
        return null;
    }

    public ShardElement commitPoint() {
        ShardElement element = null;
        for (ShardElement e : queue) {
            if (e.isOk()) {
                element = e;
            } else {
                break;
            }
        }
        return element;
    }

    public void popOKElements() {
        for (Iterator<ShardElement> it = queue.iterator(); it.hasNext(); ) {
            ShardElement e = it.next();
            if (e.isOk()) {
                it.remove();
            } else {
                break;
            }
        }
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }

    public ShardElement getShardElement(long offset) {
        for (ShardElement element : queue) {
            if (offset == element.getOffset()) return element;
        }
        return null;
    }
}
