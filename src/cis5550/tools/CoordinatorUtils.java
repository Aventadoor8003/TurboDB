package cis5550.tools;

import cis5550.model.WorkerMeta;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class CoordinatorUtils {
    private static final Logger logger = Logger.getLogger(CoordinatorUtils.class);

    public static String generateWorkerString(Map<String, WorkerMeta> workers) {
        WorkerChecker workerChecker = new WorkerChecker(workers);
        workerChecker.checkAndRemoveInactiveWorkers();
        StringBuilder sb = new StringBuilder();
        sb.append(workers.size()).append("\n");
        for (Map.Entry<String, WorkerMeta> entry : workers.entrySet()) {
            long lastUpdatedTimeMillis = entry.getValue().getLastUpdateTime().toEpochMilli();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastUpdatedTimeMillis > 15000) {
                workers.remove(entry.getKey());
                //logger.debug("Removed worker " + entry.getValue().getWorkerId() + " at " + entry.getValue().getIp() + ":" + entry.getValue().getPort());
                continue;
            }
            sb.append(entry.getKey()).append(",").append(entry.getValue().getIp()).append(":").append(entry.getValue().getPort()).append("\n");
        }
        return sb.toString();
    }

    public static String generateWorkerTable(Map<String, WorkerMeta> workers) {
        StringBuilder sb = new StringBuilder();

        // Start the HTML document.
        sb.append("<!DOCTYPE html>\n");
        sb.append("<html>\n");
        sb.append("<head>\n");
        sb.append("<title>Worker Table</title>\n");
        sb.append("</head>\n");
        sb.append("<body>\n");

        // Start the HTML table with border.
        sb.append("<table border=\"1\">\n");

        // Add table headers.
        sb.append("<tr>\n");
        sb.append("<th>ID</th>\n");
        sb.append("<th>IP</th>\n");
        sb.append("<th>Port</th>\n");
        sb.append("<th>Updated</th>\n");
        sb.append("<th>Created</th>\n");
        sb.append("<th>Link</th>\n");
        sb.append("</tr>\n");

        // Add each worker's information as a row in the table, sorted in worker id
        List<Map.Entry<String, WorkerMeta>> sortedWorkers = workers.entrySet()
                .stream()
                .sorted(Comparator.comparing(e -> e.getValue().getWorkerId()))
                .toList();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
        for (Map.Entry<String, WorkerMeta> entry : sortedWorkers) {
            WorkerMeta worker = entry.getValue();
            long lastUpdatedTimeMillis = worker.getLastUpdateTime().toEpochMilli();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastUpdatedTimeMillis > 15000) {
                workers.remove(entry.getKey());
                //logger.debug("Removed worker " + worker.getWorkerId() + " at " + worker.getIp() + ":" + worker.getPort());
                continue;
            }
            sb.append("<tr>\n");
            sb.append("<td>").append(worker.getWorkerId()).append("</td>\n");
            sb.append("<td>").append(worker.getIp()).append("</td>\n");
            sb.append("<td>").append(worker.getPort()).append("</td>\n");
            sb.append("<td>").append(worker.getLastUpdateTime().atZone(ZoneId.systemDefault()).format(formatter)).append("</td>\n");
            sb.append("<td>").append(worker.getCreateTime().atZone(ZoneId.systemDefault()).format(formatter)).append("</td>\n");
            sb.append("<td><a href=\"http://")
                    .append(worker.getIp())
                    .append(":")
                    .append(worker.getPort())
                    .append("/\">Link</a></td>\n");
            sb.append("</tr>\n");
        }


        // Close the table and body tag.
        sb.append("</table>\n");
        sb.append("</body>\n");
        sb.append("</html>\n");
        return sb.toString();
    }

    public static boolean isValidId(String id) {
        return id != null;
    }


}
