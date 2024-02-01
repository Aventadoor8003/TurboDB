package cis5550.model;

import cis5550.tools.WorkerChecker;

import java.util.*;

public record WorkerContext(Map<String, Table> tables, String storageDir, WorkerChecker checker,
                            NavigableMap<String, WorkerMeta> workers, String workerId) {
    public List<WorkerMeta> getReplicaList(String workerId) {
        Set<WorkerMeta> workerSet = new HashSet<>();
        WorkerMeta next = getNextLower(workerId);
        if (next != null && !workerId.equals(next.getWorkerId())) {
            workerSet.add(next);
        }
        if(next == null) {
            return new ArrayList<>(workerSet);
        }
        WorkerMeta nextNext = getNextLower(next.getWorkerId());
        if (nextNext != null && !workerId.equals(nextNext.getWorkerId())) {
            workerSet.add(nextNext);
        }
        return new ArrayList<>(workerSet);
    }



    private WorkerMeta getNextLower(String workerId) {
        try {
            return workerId.equals(workers.firstKey()) ? workers.lastEntry().getValue() : workers.lowerEntry(workerId).getValue();
        } catch (NoSuchElementException e) {
            return null;
        }
    }



}
