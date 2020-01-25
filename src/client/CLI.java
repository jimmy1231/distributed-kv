package client;

import app_kvClient.KVClient;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.text.MessageFormat;

public class CLI {
    private static Logger logger = Logger.getRootLogger();
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
        if(client != null) {
            client.getStore().disconnect();
            client = null;
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");
            //TODO call system exit

        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    connect(serverAddress, serverPort);
                } catch(NumberFormatException nfe) {
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
            if (tokens.length == 3) {
                if (client != null && client.isRunning()) {
                    String key = tokens[1];
                    String value = tokens[2];
                    logger.info(MessageFormat.format("Sending PUT <{0}, {1}>", key, value));

                    try{
                        client.getStore().put(key, value);
                    }
                    catch(Exception e){
                        String error_msg = MessageFormat.format("Failed to send put request with ({0}, {1})",
                                key,
                                value);
                        System.out.print(error_msg);
                        logger.error(error_msg, e);
                    }

                } else {
                    printError("Not connected!");
                }
            } else if (tokens.length > 3) {
                printError("Too many arguments");
            } else {
                printError("Too few arguments");
            }

        } else  if (tokens[0].equals("get")) {
                if(tokens.length == 2) {
                    if(client != null && client.isRunning()){
                        String key = tokens[1];
                        logger.info(MessageFormat.format("Sending GET <{0}>", key));
                        System.out.println(MessageFormat.format("Sending GET <{0}>", key));

                        try{
                            KVMessage result = client.getStore().get(key);
                            logger.info(MessageFormat.format("{0} retrieved <{1}, {2}>", result.getStatus(),
                                    result.getKey(), result.getValue()));
                            System.out.println(MessageFormat.format("{0} retrieved <{1}, {2}>",
                                    result.getStatus(), result.getKey(), result.getValue()));
                        }
                        catch (Exception e){
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

        } else if(tokens[0].equals("disconnect")) {
            disconnect();

            if (client == null){
                System.out.println(PROMPT + "Successfully disconnected from the server");
                logger.info("Client requested disconnection from the server");
            }
        } else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    public void run(){
        while(!stop) {
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

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
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
}
