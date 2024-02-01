package cis5550.kvs;

import cis5550.tools.CoordinatorUtils;
import cis5550.tools.Logger;
import cis5550.tools.WorkerChecker;
import cis5550.webserver.Server;


public class Coordinator extends cis5550.generic.Coordinator {

    private static final Logger logger = Logger.getLogger(Coordinator.class);
    private static int serverPort;

    public static void main(String[] args) {
        if (args.length != 1) {
            logger.fatal("Usage: Coordinator <port>");
            System.exit(1);
        }

        try {
            serverPort = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            logger.fatal("Usage: Coordinator <port>");
            System.exit(1);
        }

        Server.port(serverPort);
        logger.debug("start");
        registerRoutes();
        Server.get("/", (request, response) -> {
            response.type("text/html");
            return CoordinatorUtils.generateWorkerTable(workers);
        });
        WorkerChecker workerChecker = new WorkerChecker(workers);
        workerChecker.startChecking();
    }
}
