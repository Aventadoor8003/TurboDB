package cis5550.tools;

import cis5550.kvs.Row;
import cis5550.model.*;
import cis5550.webserver.Request;

import java.io.*;
import java.util.*;

public class WorkerUtils {
    private static WorkerContext workerContext;
    private static final Logger logger = Logger.getLogger(WorkerUtils.class);

    public static void setWorkerContext(WorkerContext workerContext) {
        WorkerUtils.workerContext = workerContext;
    }

    public static String generateTableHtml() {
        StringBuilder sb = new StringBuilder();

        sb.append("<!DOCTYPE html>");
        sb.append("<html>");
        sb.append("<head>");
        sb.append("<title>Data table preview</title>");
        sb.append("</head>");
        sb.append("<body>");

        sb.append("<table border='1'>");
        sb.append("<thead>");
        sb.append("<tr><th>Table name</th><th>Number of keys</th></tr>");
        sb.append("</thead>");
        sb.append("<tbody>");

        for (Map.Entry<String, Table> entry : workerContext.tables().entrySet()) {
            String tableName = entry.getKey();
            int numberOfKeys = entry.getValue().countKeys();

            sb.append("<tr>");
            sb.append("<td><a href='/view/").append(tableName).append("'>").append(tableName).append("</a></td>");
            sb.append("<td>").append(numberOfKeys).append("</td>");
            sb.append("</tr>");
        }

        sb.append("</tbody>");
        sb.append("</table>");
        sb.append("</body>");
        sb.append("</html>");

        return sb.toString();
    }

    public static String generateTableView(Table table, Request request) {
        int start = request.queryParams().contains("start") ? Integer.parseInt(request.queryParams("start")) : 0;
        logger.debug("Found start query param " + start);
        int end = Math.min(start + 10, table.countKeys());
        return generateTableView(table, start, end);
    }

    public static String generateTableView(Table table, int start, int end) {
        logger.debug("Generating table view for table " + table.getName() + " from " + start + " to " + end);
        List<String> keys = new ArrayList<>(table.getKeys());
        Collections.sort(keys);
        keys = keys.subList(start, Math.min(end, keys.size()));

        Set<String> uniqueColumns = new TreeSet<>();
        for (String key : keys) {
            Row row = table.get(key);
            uniqueColumns.addAll(row.columns());
        }

        String tableName = table.getName();
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n<html>\n<head>\n    <title>View ").append(tableName).append("</title>\n</head>\n<body>\n");
        html.append("<h1>").append(tableName).append(" View</h1>\n");
        html.append("<table border=\"1\">\n    <thead>\n        <tr>\n");
        html.append("            <th>Row Key</th>\n");
        for (String column : uniqueColumns) {
            html.append("            <th>").append(column).append("</th>\n");
        }
        html.append("        </tr>\n    </thead>\n    <tbody>\n");

        for (String key : keys) {
            Row row = table.get(key);
            html.append("        <tr>\n");
            html.append("            <td>").append(key).append("</td>\n");
            for (String column : uniqueColumns) {
                html.append("            <td>").append(row.get(column)).append("</td>\n");
            }
            html.append("        </tr>\n");
        }

        html.append("    </tbody>\n</table>\n");
        if (end < table.countKeys()) {
            html.append("<a href=\"/view/").append(tableName).append("?start=").append(end).append("\">Next</a>\n");
        }
        html.append("</body>\n</html>");

        return html.toString();
    }


    public static String putRow(String table, String rowKey, String column, byte[] data) {
        Map<String, Table> tables = workerContext.tables();
        tables.putIfAbsent(table, table.startsWith("pt-") ? new PersistentTable(table, workerContext.storageDir()) : new MemoryTable(table));
        Row rowObj = tables.get(table).get(rowKey);
        rowObj = rowObj == null ? new Row(rowKey) : rowObj.clone();
        rowObj.put(column, data);
        tables.get(table).put(rowKey, rowObj);
        return "OK";
    }

    public static Row getRow(String tableName, String rowKey, Integer version) throws NullPointerException {
        if (tableName.startsWith("pt-")) {
            return workerContext.tables().get(tableName).get(rowKey);
        } else {
            return ((MemoryTable) workerContext.tables().get(tableName)).get(rowKey, version);
        }
    }


    public static List<WorkerMeta> generateWorkerList(String input) {
        String[] workerInfo = input.split(" ");
        List<WorkerMeta> workerList = new ArrayList<>();
        for(int i = 1; i < workerInfo.length; i++) {
            workerList.add(generateWorkerMeta(workerInfo[i]));
        }
        return workerList;
    }
    private static WorkerMeta generateWorkerMeta(String info) {
        String[] parts = info.split(",");
        String[] ipPort = parts[1].trim().split(":");
        String workerId = parts[0].trim();
        String ip = ipPort[0].trim();
        int port = Integer.parseInt(ipPort[1].trim());

        return new WorkerMeta(ip, port, workerId);
    }

    public static byte[] serializeList(ArrayList<String> list) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(list);
            oos.flush();
            return bos.toByteArray();
        } catch (Exception e) {
            logger.error("Failed to serialize list", e);
            return null;
        }
    }

    public static ArrayList<String> deserializeList(byte[] data) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (ArrayList<String>) ois.readObject();
        } catch (Exception e) {
            logger.error("Failed to deserialize data", e);
            return null;
        }
    }

}
