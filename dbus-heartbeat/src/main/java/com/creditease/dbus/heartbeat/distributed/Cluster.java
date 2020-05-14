package com.creditease.dbus.heartbeat.distributed;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Cluster {

    private List<Worker> workers;
    private Map<Integer, Worker> workersById;

    public Cluster(List<Worker> workers) {
        this.workers = workers;
        workersById = new HashMap<>();
        Optional.ofNullable(workers).orElseGet(Collections::emptyList).stream().forEach(worker -> workersById.put(worker.getId(), worker));
    }

    public List<Worker> getWorkers() {
        return workers;
    }

    public int workerCount() {
        return workers.size();
    }
}
