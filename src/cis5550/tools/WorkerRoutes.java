package cis5550.tools;

import cis5550.kvs.Row;
import cis5550.model.*;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerRoutes {
    private static final Logger logger = Logger.getLogger(WorkerRoutes.class);
    private static WorkerContext workerContext;
    private static final Map<String, Object> locks = new ConcurrentHashMap<>();


    public static void setWorkerContext(WorkerContext workerContext) {
        WorkerRoutes.workerContext = workerContext;
    }

    public static String putData(Request request, Response response) {
        String table = request.params("table");
        String row = request.params("row");
        String column = request.params("column");
        //logger.debug("[kv worker] Received PUT request for table " + table + ", row " + row + ", column " + column);
        String end = request.queryParams("end");

        String ifColumn = request.queryParams("ifcolumn");
        String equals = request.queryParams("equals");

        if (ifColumn != null && equals != null) {
            logger.debug("Trying conditional PUT");
            Row rowObj = WorkerUtils.getRow(table, row, null);
            if (rowObj == null) {
                response.status(200, "FAIL");
                return "FAIL";
            }
            byte[] columnValue = rowObj.getBytes(ifColumn);
            if (columnValue == null || !equals.equals(new String(columnValue))) {
                response.status(200, "FAIL");
                return "FAIL";
            }
        }
        byte[] data = request.bodyAsBytes();
        logger.debug("PUT data: " + new String(data));
        String returnVal = WorkerUtils.putRow(table, row, column, data);

        Table tableObj = workerContext.tables().get(table);
        if (tableObj instanceof MemoryTable) {
            response.header("Version", (((MemoryTable) tableObj).newestVersion(row) + ""));
        }

        if(end != null) {
            logger.debug("Replication put, no replication executed");
            return returnVal;
        }

        //Replication
        logger.debug("Replication put, executing replication");
        workerContext.checker().checkAndRemoveInactiveWorkers();
        List<WorkerMeta> replicationList = workerContext.getReplicaList(workerContext.workerId());
        logger.debug("Replication list: " + replicationList);
        for (WorkerMeta worker : replicationList) {
            HttpURLConnection connection = null;
            try {
                String urlStr = String.format("http://%s:%d/data/%s/%s/%s?end=1",
                        worker.getIp(),
                        worker.getPort(),
                        table, row, column);
                logger.debug("Replicating data to " + urlStr);

                URL url = URI.create(urlStr).toURL();
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("PUT");
                connection.setRequestProperty("Content-Type", "application/octet-stream");
                connection.setDoOutput(true);

                try (OutputStream os = connection.getOutputStream()) {
                    os.write(data);
                    os.flush();
                }

                int responseCode = connection.getResponseCode();
                if (responseCode != HttpURLConnection.HTTP_OK) {
                    logger.error("Failed to replicate data to worker. HTTP Response Code: " + responseCode);
                }

            } catch (IOException e) {
                logger.error("Error while replicating data to worker", e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }
        return returnVal;
    }


    public static String getData(Request request, Response response) {
        String table = request.params("table");
        String row = request.params("row");
        String column = request.params("column");
        logger.debug("[kv worker] Received GET request for table " + table + ", row " + row + ", column " + column);

        String versionParam = request.queryParams("version");
        Integer version = null;
        if (versionParam != null) {
            try {
                version = Integer.parseInt(versionParam);
                logger.debug("Version: " + version);
            } catch (NumberFormatException e) {
                response.status(400, "BAD REQUEST");
                return "Invalid version number";
            }
        }

        Map<String, Table> tables = workerContext.tables();
        if (tables.get(table) == null) {
            response.status(404, "NOT FOUND");
            return "Table NOT FOUND";
        }

        Row rowObj;
        try {
            rowObj = WorkerUtils.getRow(table, row, version);
        } catch (NullPointerException e) {
            response.status(404, "NOT FOUND");
            return "Table NOT FOUND";
        }
        if (rowObj == null) {
            response.status(404, "NOT FOUND");
            return "Row NOT FOUND";
        }
        byte[] data = rowObj.getBytes(column);
        if (data == null) {
            response.status(404, "NOT FOUND");
            return "Column NOT FOUND";
        }
        if (!table.startsWith("pt-") && tables.get(table) != null) {
            response.header("Version", version == null ? ((MemoryTable) tables.get(table)).newestVersion(row) + "" : version + "s");
        }
        logger.debug("GET data: " + new String(data));
        response.bodyAsBytes(data);
        return null;
    }

    public static String sendRow(Request request, Response response) {
        //logger.debug("[worker] Trying to read row");
        String tableName = request.params("table");
        String rowKey = request.params("row");

        Row row = null;
        Table tableObj = workerContext.tables().get(tableName);
        if (tableObj != null) {
            row = tableObj.get(rowKey);
        }

//        if (row == null && tableName.startsWith("pt-")) {
//            row = WorkerUtils.readRowFromDisk(tableName, rowKey);
//        }

        if (row == null) {
            response.status(404, "NOT FOUND");
            return "Row not found.";
        }

        //logger.debug("[worker] Row found: " + row);
        byte[] serializedRow = row.toByteArray();
        logger.debug("[worker] Serialized row: " + new String(serializedRow));
        response.type("application/octet-stream");
        response.bodyAsBytes(serializedRow);
        return null;
    }


    public static String sendTable(Request req, Response res) throws Exception {
        String table = req.params("table");
        String startRow = req.queryParams("startRow");
        String endRowExclusive = req.queryParams("endRowExclusive");

        Map<String, Table> tables = workerContext.tables();
        if (!table.startsWith("pt-") && tables.get(table) != null) {
            logger.debug("Table: " + table + " exists in memory");
            Table tableObj = tables.get(table);
            for (String rowKey : tableObj.getKeys()) {
                if ((startRow == null || rowKey.compareTo(startRow) >= 0) &&
                        (endRowExclusive == null || rowKey.compareTo(endRowExclusive) < 0)) {
                    Row row = tableObj.get(rowKey);
                    res.write(row.toByteArray());
                    res.write("\n".getBytes());
                }
            }
            res.write("\n".getBytes());
            return null;
        }

        File tableDir = new File(workerContext.storageDir(), table);
        if (!tableDir.exists() || !tableDir.isDirectory()) {
            res.status(404, "NOT FOUND");
            return "NOT FOUND";
        }

        //logger.debug("Table dir: " + tableDir.getAbsolutePath());
        File[] files = tableDir.listFiles();
        if (files == null) {
            logger.error("Error to open directory " + tableDir.getAbsolutePath());
            res.status(404, "NOT FOUND");
            return null;
        }
        res.type("text/plain");
        int fileCount = 0;
        //TODO: Notice that row is loaded into memory entirely. Improve it to load name first, and only load the row when it will be sent
        for (File file : files) {
            Row row = Row.readFrom(new FileInputStream(file));
            //logger.debug("Row: " + row);
            if (row == null) {
                logger.warn("Row " + file.getName() + " is null");
                continue;
            }
            String rowKey = row.key();
            if ((startRow == null || rowKey.compareTo(startRow) >= 0) &&
                    (endRowExclusive == null || rowKey.compareTo(endRowExclusive) < 0)) {
                res.write(row.toByteArray());
                res.write("\n".getBytes());
                fileCount++;
            }
        }
        if (fileCount == 0) {
            logger.error("No files found " + tableDir.getAbsolutePath() + " startRow: " + startRow + " endRowExclusive: " + endRowExclusive);
            res.status(404, "NOT FOUND");
            return null;
        }
        res.write("\n".getBytes());
        return null;
    }

    public static String countKeys(Request request, Response response) {
        String tableName = request.params("table");
        if (tableName == null) {
            response.status(400, "BAD REQUEST");
            return "Param table not found.";
        }
        Table tableObj = workerContext.tables().get(tableName);
        if (tableObj == null) {
            response.status(404, "NOT FOUND");
            return "Table not found.";
        }
        return String.valueOf(tableObj.countKeys());
    }

    public static String renameTable(Request req, Response res) {
        String newName = req.body();
        String oldName = req.params("table");
        Map<String, Table> tables = workerContext.tables();
        if (!tables.containsKey(oldName)) {
            res.status(404, "NOT FOUND");
            return "Table " + oldName + " does not exist";
        }
        if (newName == null || newName.isEmpty()) {
            res.status(400, "BAD REQUEST");
            return "New name cannot be empty";
        }


        if (tables.containsKey(newName)) {
            res.status(409, "CONFLICT");
            return "Table " + newName + " already exists";
        }

        if (TableType.getType(oldName) != TableType.getType(newName)) {
            res.status(400, "BAD REQUEST");
            return "Cannot rename table from " + oldName + " to " + newName + " because they are of different types";
        }

        Object lock = locks.computeIfAbsent(oldName, k -> new Object());
        synchronized (lock) {
            Table oldTable = tables.get(oldName);
            if (oldTable instanceof MemoryTable) {
                logger.debug("Renaming in-memory table " + oldName + " to " + newName);
                tables.put(newName, oldTable);
                tables.remove(oldName);
                oldTable.setName(newName);
                res.status(200, "OK");
                return "OK";
            }

            String storageDir = workerContext.storageDir();
            Path oldPath = Paths.get(storageDir, oldName);
            Path newPath = Paths.get(storageDir, newName);
            if (!Files.exists(oldPath)) {
                res.status(404, "NOT FOUND");
                return "Table " + oldName + " does not exist on disk";
            }
            try {
                Files.move(oldPath, newPath);
                res.status(200, "OK");
                tables.put(newName, oldTable);
                tables.remove(oldName);
                oldTable.setName(newName);
                res.status(200, "OK");
                return "OK";
            } catch (IOException e) {
                logger.error("Error while renaming table " + oldName + " to " + newName, e);
                res.status(500, "INTERNAL SERVER ERROR");
                return "Error while renaming table " + oldName + " to " + newName;
            }
        }
    }

    public static String deleteTable(Request req, Response res) {
        Map<String, Table> tables = workerContext.tables();
        String tableName = req.params("table");
        Table table = tables.get(tableName);
        if (table == null) {
            res.status(404, "NOT FOUND");
            return "Table " + tableName + " not found";
        }
        synchronized (table) {
            if (table instanceof PersistentTable) {
                try {
                    ((PersistentTable) table).destroy();
                } catch (IOException e) {
                    logger.error("Error while deleting table " + tableName, e);
                    res.status(500, "INTERNAL SERVER ERROR");
                    return "Error while deleting table " + tableName;
                }
            }
            tables.remove(tableName);
        }
        return null;
    }

}
