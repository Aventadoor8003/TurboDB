package cis5550.generic;

import cis5550.model.WorkerMeta;
import cis5550.tools.CoordinatorUtils;
import cis5550.tools.Logger;
import cis5550.tools.WorkerChecker;
import cis5550.webserver.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static cis5550.tools.CoordinatorUtils.*;

public class Coordinator {
    private static final Logger logger = Logger.getLogger(Coordinator.class);
    protected static final Map<String, WorkerMeta> workers = new ConcurrentHashMap<>();
    public static List<String> getWorkers() {
        WorkerChecker checker = new WorkerChecker(workers);
        checker.checkAndRemoveInactiveWorkers();
        List<String> workerList = new ArrayList<>();
        workerList.add(workers.size() + "");
        for(var entry : workers.entrySet()) {
            workerList.add(entry.getValue().getIp() + ":" + entry.getValue().getPort());
        }
        return workerList;
    }

    public static String workTable() {
        return CoordinatorUtils.generateWorkerTable(workers);
    }

    public static void registerRoutes() {
        Server.get("/ping", (request, response) -> {
            //logger.debug("Added ping to server");
            String ip = request.ip();
            int port;
            try {
                port = Integer.parseInt(request.queryParams("port"));
            } catch (NumberFormatException e) {
                logger.error("Error while parsing port", e);
                response.status(400, "Bad request");
                return "Bad request";
            }

            String workerId = request.queryParams("id");
            if(!CoordinatorUtils.isValidId(workerId)) {
                response.status(400, "Bad request");
                return "Bad request";
            }
            //logger.debug("Received ping from worker " + workerId + " at " + ip + ":" + port);
            if(!workers.containsKey(workerId)) {
                WorkerMeta worker = new WorkerMeta(ip, port, workerId);
                workers.put(workerId, worker);
                //logger.debug("Added worker " + workerId + " at " + worker.getCreateTime());
            } else {
                workers.get(workerId).updateLastUpdateTime();
                //logger.debug("Updated worker " + workerId + " at " + workers.get(workerId).getLastUpdateTime());
            }
            return "OK";
        });
        Server.get("/workers", (request, response) -> generateWorkerString(workers));
    }
}


