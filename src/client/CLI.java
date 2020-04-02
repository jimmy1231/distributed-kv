package client;

import app_kvClient.KVClient;
import logger.LogSetup;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.*;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Objects;

public class CLI {
    private static final Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private boolean stop = false;
    private KVClient client = null;

    private String serverAddress;
    private int serverPort;

    private void connect(String address, int port) throws Exception {
        client = new KVClient();
        client.newConnection(address, port);
    }

    private void disconnect() {
        if (client != null) {
            client.getStore().disconnect();
            client = null;
        }
    }

    private void putKV(String key, String value) {
        if (value.equals("null")) {
            value = null;
        }

        if (client != null && client.isRunning()) {
            logger.info(MessageFormat.format("Sending PUT <{0}, {1}>", key, value));
            System.out.println(MessageFormat.format("Sending PUT <{0}, {1}>", key, value));

            try {
                KVMessage result = client.getStore().put(key, value);
                logger.info(MessageFormat.format("{0} Inserted <{1}, {2}>", result.getStatus(),
                    result.getKey(), result.getValue()));
                System.out.println(MessageFormat.format("{0} Inserted <{1}, {2}>",
                    result.getStatus(), result.getKey(), result.getValue()));
            } catch (Exception e) {
                String error_msg = MessageFormat.format("Request failed: PUT <{1}, {2}>",
                    key,
                    value);
                System.out.print(error_msg);
                logger.error(error_msg, e);
            }

        } else {
            printError("Not connected!");
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");
            //TODO call system exit

        } else if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                try {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    connect(serverAddress, serverPort);
                } catch (NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                printError("Invalid number of parameters! Expected connect <address> <port>");
            }

        } else if (tokens[0].equals("put")) {
            if (tokens.length >= 2) {
                String key = tokens[1];
                String value;

                if (tokens.length == 2) {
                    System.out.println("No value for PUT request given. It may delete the entry you are looking for!");
                }

                // Handle value with spaces
                tokens = Arrays.copyOfRange(tokens, 2, tokens.length);
                value = String.join(" ", tokens);

                // string of null should be considered an empty string
               putKV(key, value);
            } else if (tokens.length > 3) {
                printError("Too many arguments");
            } else {
                printError("Too few arguments");
            }

        } else if (tokens[0].equals("test1")) {
            putKV("hello", "world");
            putKV("disney", "land");
            putKV("walt", "disney");
            putKV("water", "bottle");
            putKV("b", "ts");
            putKV("loki", "watson");
            putKV("watson", "loki");
            putKV("baby", "bear");
            putKV("pls", "help");
            putKV("hashy", "oats");
            putKV("nogucci", "gang");
        } else if (tokens[0].equals("get")) {
            if (tokens.length == 2) {
                if (client != null && client.isRunning()) {
                    String key = tokens[1];
                    logger.info(MessageFormat.format("Sending GET <{0}>", key));
                    System.out.println(MessageFormat.format("Sending GET <{0}>", key));

                    try {
                        KVMessage result = client.getStore().get(key);
                        logger.info(MessageFormat.format("{0} retrieved <{1}, {2}>", result.getStatus(),
                            result.getKey(), result.getValue()));
                        System.out.println(MessageFormat.format("{0} retrieved <{1}, {2}>",
                            result.getStatus(), result.getKey(), result.getValue()));
                    } catch (Exception e) {
                        String error_msg = MessageFormat.format("Request failed: GET <{1}>", key);
                        System.out.print(error_msg);
                        logger.error(error_msg, e);
                    }

                } else {
                    printError("Not connected!");
                }
            } else if (tokens.length > 2) {
                printError("Too many arguments");
            } else {
                printError("Too few arguments");
            }

        } else if (tokens[0].equals("disconnect")) {
            disconnect();

            if (client == null) {
                System.out.println(PROMPT + "Successfully disconnected from the server");
                logger.info("Client requested disconnection from the server");
            }
        } else if (tokens[0].equals("logLevel")) {
            if (tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                        "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }
        } else if (tokens[0].equals("help")) {
            printHelp();
        } else if (tokens[0].equals("print_ring")) {
            client.getStore().printRing();
        } else if (tokens[0].equals("mapreduce")) {
            if (tokens.length < 2) {
                printError("Too few arguments");
            }
            String[] keys = ArrayUtils.subarray(tokens, 1, tokens.length);
            try {
                client.getStore().mapReduce(keys);
            } catch (Exception e) {
                String error_msg = MessageFormat.format("Request failed: MapReduce <{1}>", keys.toString());
                System.out.print(error_msg);
                logger.error(error_msg, e);
            }
        } else if (tokens[0].equals("putf")) {
            if (tokens.length != 3 || Objects.isNull(tokens[1])
                || Objects.isNull(tokens[2])) {
                printError("Invalid number of arguments");
                return;
            }
            String fileContents = readfile(tokens[2]);
            if (Objects.isNull(fileContents)) {
                logger.error("File is invalid!");
                return;
            }

            putKV(tokens[1], fileContents);
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private String setLevel(String levelString) {
        if (levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("send <text message>");
        sb.append("\t\t sends a text message to the server \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
            + "Possible log levels are:");
        System.out.println(PROMPT
            + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String readfile(String path) {
        File file = new File(path);
        if (!file.exists()) {
            return null;
        }

        StringBuilder builder = new StringBuilder();
        try {
            BufferedReader r = new BufferedReader(new FileReader(file));

            String line;
            while ((line = r.readLine()) != null) {
                builder.append(line).append(" ");
            }

        } catch (Exception e) {
            return null;
        }

        return builder.toString().trim();
    }
}
