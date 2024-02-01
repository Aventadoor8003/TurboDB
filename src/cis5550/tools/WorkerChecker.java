package cis5550.tools;

import cis5550.model.WorkerMeta;

import java.util.Map;
import java.util.concurrent.*;

public class WorkerChecker {
    //private static final Logger logger = Logger.getLogger(WorkerChecker.class);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Map<String, WorkerMeta> workers;

    public WorkerChecker(Map<String, WorkerMeta> workers) {
        this.workers = workers;
    }

    public void startChecking() {
        final Runnable checker = this::checkAndRemoveInactiveWorkers;
        scheduler.scheduleAtFixedRate(checker, 0, 15, TimeUnit.SECONDS);
    }

    public void checkAndRemoveInactiveWorkers() {
        long currentTime = System.currentTimeMillis();
        workers.entrySet().removeIf(entry -> currentTime - entry.getValue().getLastUpdateTime().toEpochMilli() > 15000);

    }
}
