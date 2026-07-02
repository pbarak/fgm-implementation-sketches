package fgm.coordinator.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import fgm.worker.remoteobject.NodeRMIInterface;

public class RMINetworkDispatcher {
    
    // 1. THE TRUE GLOBAL LOCK
    public static final Object GLOBAL_PHASE_LOCK = new Object();

    private static final Map<NodeRMIInterface<?>, ExecutorService> workerQueues = new ConcurrentHashMap<>();

    public static void dispatch(NodeRMIInterface<?> worker, Runnable rmiTask) {
        workerQueues.computeIfAbsent(worker, k -> Executors.newSingleThreadExecutor())
                    .submit(rmiTask);
    }
    
    public static void shutdownAll() {
        workerQueues.values().forEach(ExecutorService::shutdownNow);
    }
}