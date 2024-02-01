package cis5550.generic;

import cis5550.tools.Logger;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Worker {
    protected static int workerPort;
    protected static IpPort masterIpPort;
    private static final Logger logger = Logger.getLogger(Worker.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    protected static String workerId;

    protected static void startPingThread() {
        Runnable pingTask = () -> {
            HttpURLConnection connection = null;
            try {
                String urlStr = String.format("http://%s/ping?id=%s&port=%d", masterIpPort.toString(), workerId, workerPort);
                logger.debug("Pinging master at " + urlStr);
                URL url = URI.create(urlStr).toURL();
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.connect();
                int responseCode = connection.getResponseCode();
                if (responseCode != HttpURLConnection.HTTP_OK) {
                    // Handle the error case. You can log the unexpected response code or take other actions.
                    logger.warn("Unexpected response from master: " + responseCode);
                }
                Thread.sleep(5000);
            } catch (InterruptedException | IOException e) {
                logger.error("Error while pinging master", e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        };
        scheduler.scheduleAtFixedRate(pingTask, 0, 5, TimeUnit.SECONDS);
    }

    public record IpPort(String ip, int port) {
        public String toString() {
            return ip + ":" + port;
        }
    }

    protected static String generateId() {
        Random random = new Random();
        StringBuilder randomStr = new StringBuilder(5);
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        for (int i = 0; i < 5; i++) {
            char randomChar = alphabet.charAt(random.nextInt(alphabet.length()));
            randomStr.append(randomChar);
        }
        return randomStr.toString();
    }
}

