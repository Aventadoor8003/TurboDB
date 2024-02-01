package cis5550.model;

import java.time.Instant;

public class WorkerMeta {
    private final String ip;
    private final int port;
    private final Instant createTime;
    private Instant lastUpdateTime;

    public WorkerMeta(String ip, int port, String workerId) {
        this.ip = ip;
        this.port = port;
        this.workerId = workerId;
        this.createTime = Instant.now();
        this.lastUpdateTime = Instant.now();
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void updateLastUpdateTime() {
        this.lastUpdateTime = Instant.now();
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getWorkerId() {
        return workerId;
    }

    private final String workerId;

    public Instant getCreateTime() {
        return createTime;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        WorkerMeta worker = (WorkerMeta) obj;
        return workerId.equals(worker.getWorkerId());
    }

    @Override
    public String toString() {
        return "WorkerMeta{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                ", workerId='" + workerId + '\'' +
                '}';
    }

}
