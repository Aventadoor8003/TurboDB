package cis5550.kvs;

import cis5550.model.PersistentTable;
import cis5550.model.Table;
import cis5550.model.WorkerContext;
import cis5550.model.WorkerMeta;
import cis5550.tools.Logger;
import cis5550.tools.WorkerChecker;
import cis5550.tools.WorkerUtils;
import cis5550.webserver.Server;
import cis5550.tools.WorkerRoutes;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

//TODO: Monitor tables in the disk, when they are deleted, just delete metadata in the memory
public class Worker extends cis5550.generic.Worker {
    private static final Logger logger = Logger.getLogger(Worker.class);
    private static final Map<String, Table> tables = new ConcurrentHashMap<>();
    private static String storageDir;
    protected static final ConcurrentNavigableMap<String, WorkerMeta> workers = new ConcurrentSkipListMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);


    public static void main(String[] args) {
        if (args.length != 3) {
            logger.fatal("Usage: Worker <port> <master-hostname> <master-port>");
            System.exit(1);
        }

        workerPort = Integer.parseInt(args[0]);
        Server.port(workerPort);
        String inputDir = args[1];
        String[] masterAddr = args[2].split(":");
        masterIpPort = new IpPort(masterAddr[0], Integer.parseInt(masterAddr[1]));
        logger.debug("Running on port " + workerPort + " with storage directory " + inputDir + " and master port " + masterIpPort);
        Path idFilePath = Paths.get(inputDir, "id");
        try {
            if (Files.exists(idFilePath)) {
                workerId = Files.readString(idFilePath);
            } else {
                workerId = generateId();
                Files.writeString(idFilePath, workerId);
            }
        } catch (IOException e) {
            logger.fatal("Error while reading worker ID", e);
            System.exit(1);
        }
        Worker.storageDir = inputDir;

        //Load persistent tables
        File dir = new File(storageDir);
        if (!dir.exists() && !dir.isDirectory()) {
            logger.fatal("Storage directory " + storageDir + " does not exist or is not a directory");
            System.exit(1);
        }
        File[] folders = dir.listFiles(File::isDirectory);
        if(folders == null){
            logger.fatal("Storage directory " + storageDir + " is empty");
            System.exit(1);
        }
        for(File folder : folders){
            String tableName = folder.getName();
            tables.put(tableName, new PersistentTable(tableName, storageDir));
        }

        //Initialize basic operations
        startPingThread();
        startReplicationThread();
        WorkerChecker workerChecker = new WorkerChecker(workers);
        workerChecker.startChecking();

        //Inject workerContext into other tools
        WorkerContext workerContext = new WorkerContext(tables, storageDir, workerChecker, workers, workerId);
        WorkerUtils.setWorkerContext(workerContext);
        WorkerRoutes.setWorkerContext(workerContext);

        Server.put("/data/:table/:row/:column", WorkerRoutes::putData);
        Server.get("/data/:table/:row/:column", WorkerRoutes::getData);
        Server.get("/", (req, res) -> WorkerUtils.generateTableHtml());
        Server.get("/view/:table", (req, res) -> WorkerUtils.generateTableView(tables.get(req.params("table")), req));
        Server.get("/data/:table/:row", WorkerRoutes::sendRow);
        Server.get("/data/:table", WorkerRoutes::sendTable);
        Server.get("/count/:table", WorkerRoutes::countKeys);
        Server.put("/rename/:table", WorkerRoutes::renameTable);
        Server.put("/delete/:table", WorkerRoutes::deleteTable);
        Server.get("/list", (req, res) -> {
            ArrayList<String> tableList = new ArrayList<>(tables.keySet());
            res.write(WorkerUtils.serializeList(tableList));
            return null;
        });
    }

    private static void startReplicationThread() {
        final Runnable workerUpdater = () -> {
            try {
                replicate();
            } catch (IOException | InterruptedException e) {
                logger.error("Error updating workers", e);
            }
        };

        scheduler.scheduleAtFixedRate(workerUpdater, 0, 5, TimeUnit.SECONDS);
    }


    private static void replicate() throws IOException, InterruptedException {
        String urlStr = String.format("http://%s/workers", masterIpPort.toString());
        logger.debug("Trying to get worker list master at " + urlStr);
        URL url = URI.create(urlStr).toURL();

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
                response.append(" ");
            }
            in.close();

            logger.debug("Response from master: " + response);
            List<WorkerMeta> workerList = WorkerUtils.generateWorkerList(response.toString());
            for(WorkerMeta worker : workerList) {
                if(!workers.containsKey(worker.getWorkerId())) {
                    workers.put(worker.getWorkerId(), worker);
                    logger.debug("Added worker " + worker.getWorkerId() + " at " + worker.getCreateTime());
                } else {
                    workers.get(worker.getWorkerId()).updateLastUpdateTime();
                    logger.debug("Updated worker " + worker.getWorkerId() + " at " + workers.get(worker.getWorkerId()).getLastUpdateTime());
                }
            }
        } else {
            logger.error("Failed to get worker list. HTTP Response Code: " + responseCode);
        }

        logger.debug("Current workers: " + workers);
    }

    private static void startUpdateThread() {

    }

    private static void update() {
        List<WorkerMeta> workerList = getPrimaryList(workerId);
        workerList.sort(Comparator.comparing(WorkerMeta::getWorkerId));
        WorkerMeta primary = workerList.get(workerList.size() - 1);
        WorkerMeta replica = workerList.get(0);
        byte[] byteArray = tryConnectAndGetList(primary);
        if (byteArray == null) {
            byteArray = tryConnectAndGetList(replica);
        }
        if (byteArray == null) {
            logger.error("Failed to get list from any worker");
            return;
        }

        List<String> tableList = WorkerUtils.deserializeList(byteArray);
        if(tableList == null) {
            logger.error("Failed to deserialize list");
            return;
        }
        for(String table : tableList) {

        }
    }

    public static List<WorkerMeta> getPrimaryList(String workerId) {
        Set<WorkerMeta> workerSet = new HashSet<>();
        WorkerMeta next = getNextHigher(workerId);
        if (next != null && !workerId.equals(next.getWorkerId())) {
            workerSet.add(next);
        }
        if(next == null) {
            return new ArrayList<>(workerSet);
        }
        WorkerMeta nextNext = getNextHigher(next.getWorkerId());
        if (nextNext != null && !workerId.equals(nextNext.getWorkerId())) {
            workerSet.add(nextNext);
        }
        return new ArrayList<>(workerSet);
    }

    private static WorkerMeta getNextHigher(String workerId) {
        if (workers.isEmpty()) {
            return null;
        }

        if (workerId.equals(workers.lastKey())) {
            return workers.firstEntry().getValue();
        }

        Map.Entry<String, WorkerMeta> higherEntry = workers.higherEntry(workerId);
        return (higherEntry != null) ? higherEntry.getValue() : null;
    }

    private static byte[] tryConnectAndGetList(WorkerMeta workerMeta) {
        try {
            URL url = new URL("http://" + workerMeta.getIp() + ":" + workerMeta.getPort() + "/list");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(300000);
            connection.setReadTimeout(300000);
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                InputStream inputStream = connection.getInputStream();
                // Convert input stream to byte array. For simplicity, assuming the list isn't too large.
                byte[] byteArray = inputStream.readAllBytes();
                inputStream.close();
                return byteArray;
            } else {
                System.err.println("Failed to get list from worker: " + workerMeta.getWorkerId() + ". HTTP Response Code: " + responseCode);
            }
        } catch (Exception e) {
            System.err.println("Error connecting to worker: " + workerMeta.getWorkerId());
            logger.error("Error connecting to worker: " + workerMeta.getWorkerId(), e);
        }
        return null;
    }

}
