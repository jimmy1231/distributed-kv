package app_kvServer;

import app_kvECS.TCPSockModule;
import app_kvECS.KVServerMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import ecs.IECSNode;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.UnifiedMessage;

import java.io.*;
import java.net.Socket;
import java.text.MessageFormat;
import java.util.Objects;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection extends Thread {
    private String id;
    private static Logger logger = Logger.getRootLogger();

    private volatile boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private KVServer server;
    private InputStream input;
    private PrintWriter output;
    private ObjectMapper objectMapper;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(String id, Socket clientSocket, KVServer server) {
        this.id = id;
        this.clientSocket = clientSocket;
        this.server = server;
        input = null;
        output = null;
        objectMapper = null;
        isOpen = true;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = new PrintWriter(clientSocket.getOutputStream(), true);
            input = clientSocket.getInputStream();
            objectMapper = new ObjectMapper();

            output.println(String.format(
                "Connection to KVServer established: %s:%s",
                clientSocket.getLocalAddress(),
                clientSocket.getLocalPort()
            ));

            while(isOpen) {
                try {
                    UnifiedMessage request = receiveMessage2();
                    if (Objects.isNull(request)) {
                        continue;
                    }

                    UnifiedMessage response;
                    switch(request.getMessageType()) {
                        case CLIENT_TO_SERVER:
                        case SERVER_TO_CLIENT:
                            logger.info("SERVER_TO_CLIENT request: " + request.serialize());
                            response = handleMessage(request);
                            break;
                        case ECS_TO_SERVER:
                        case SERVER_TO_ECES:
                            logger.info("ECS_TO_SERVER request: " + request.serialize());
                            response = handleAdminMessage(request);
                            break;
                        case SERVER_TO_SERVER:
                            logger.info("SERVER_TO_SERVER request: " + request.serialize());
                            response = handleServerMessage(request);
                            break;
                        default:
                            logger.info("UNKNOWN request: " + request.serialize());
                            throw new Exception("Invalid message type");
                    }

                    sendMessage(response);
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!"); //This message gets printed out when client disconnect
                    isOpen = false;
                } catch (Exception e) {
                    logger.error("Failed to handle the request and send the reply: ", e);
                }
            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);

        } finally {
            System.out.printf("CLOSING THREAD=%s\n", getId());
            server.closeConnection(id);
            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    objectMapper = null;
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    private UnifiedMessage handleMessage(UnifiedMessage msg) {
        String key = msg.getKey();
        String value = msg.getValue();
        KVMessage.StatusType status = null;
        UnifiedMessage replyMsg = msg;
        boolean started = IECSNode.ECSNodeFlag.START.equals(server.getStatus());

        if (msg.getStatusType() == KVMessage.StatusType.PUT) {
            if (!started) {
                System.out.println("SERVER IS STOPPED!");
                status = KVMessage.StatusType.SERVER_STOPPED;
            } else {
                String infoMsg = MessageFormat.format("Received PUT <{0}, {1}>", key, value);
                logger.info(infoMsg);
                System.out.println(infoMsg);

                try{
                    status = server.putKVWithStatusCheck(key, value);
                    String successMsg = MessageFormat.format("{0} <{1}, {2}>", status, key, value);
                    logger.info(successMsg);
                    System.out.println(successMsg);
                }
                catch (Exception e){
                    System.out.println("Exception!" + key + " " + value);
                    // Delete scenario
                    if (value == null || value == "null" || value == "") {
                        status = KVMessage.StatusType.DELETE_ERROR;
                    }
                    else{
                        status =  KVMessage.StatusType.PUT_ERROR;
                    }

                    String failMsg = MessageFormat.format("{0} Failed to put <{1}, {2}>",
                            msg.getStatusType(),
                            key,
                            value);
                    logger.warn(failMsg);
                    System.out.println(failMsg);
                }
            }
        }

        else if (msg.getStatusType() == KVMessage.StatusType.GET){
            if (!started) {
                status = KVMessage.StatusType.SERVER_STOPPED;
            } else {
                String infoMsg = MessageFormat.format("Received GET <{0}>", msg.getKey());
                logger.info(infoMsg);
                System.out.println(infoMsg);

                try{
                    String retrievedValue = this.server.getKV(key);
                    status = KVMessage.StatusType.GET_SUCCESS;
                    replyMsg.setValue(retrievedValue);

                    String successMsg = MessageFormat.format("{0} <{0}, {1}>", status, key, retrievedValue);
                    logger.info(successMsg);
                    System.out.println(successMsg);
                }
                catch (Exception e){
                    status = KVMessage.StatusType.GET_ERROR;
                    String failMsg = MessageFormat.format("{0} Failed to find the value for key <{1}>",
                            msg.getStatusType(),
                            msg.getKey());
                    logger.warn(failMsg);
                    System.out.println(failMsg);
                }
            }
        }

        replyMsg.setStatusType(status);
        return replyMsg;
    }

    private UnifiedMessage handleAdminMessage(UnifiedMessage msg) {
        UnifiedMessage replyMsg = msg;
        KVMessage.StatusType status = msg.getStatusType();
        KVServerMetadata metadata = msg.getMetadata();
        System.out.println(status);
        if (KVMessage.StatusType.START.equals(status)) {
            server.start();
            server.update(metadata);
            status = KVMessage.StatusType.SUCCESS;
        }
        else if (KVMessage.StatusType.STOP.equals(status)) {
            server.stop();
            server.update(metadata);
            status = KVMessage.StatusType.SUCCESS;
        }
        else if (KVMessage.StatusType.SHUTDOWN.equals(status)) {
            server.shutdown();
            server.update(metadata);
            status = KVMessage.StatusType.SUCCESS;
        }
        else if (KVMessage.StatusType.SERVER_INIT.equals(status)){
            logger.info("REACHED SERVER_INIT");
            server.initKVServer(msg.getMetadata(), msg.getCacheSize(), msg.getCacheStrategy());
            status = KVMessage.StatusType.SUCCESS;
        }
        else if (KVMessage.StatusType.SERVER_WRITE_LOCK.equals(status)){
            server.lockWrite();
            status = KVMessage.StatusType.SUCCESS;
        }
        else if (KVMessage.StatusType.SERVER_WRITE_UNLOCK.equals(status)){
            server.unLockWrite();
            status = KVMessage.StatusType.SUCCESS;
        }
        else if (KVMessage.StatusType.SERVER_MOVEDATA.equals(status)){
            server.moveData(msg.getKeyRange(), msg.getServer());
            status = KVMessage.StatusType.SUCCESS;
        }
        else if (KVMessage.StatusType.SERVER_UPDATE.equals(status)){
            server.update(msg.getMetadata());
            status = KVMessage.StatusType.SUCCESS;
        }

        replyMsg.setStatusType(status);
        return replyMsg;
    }

    private UnifiedMessage handleServerMessage(UnifiedMessage msg) {
        if (msg.getStatusType().equals(KVMessage.StatusType.SERVER_MOVEDATA)) {
            server.recvData(msg.getDataSet());
        }

        return msg;
    }

    /**
     * Method sends a Message using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(UnifiedMessage msg) throws Exception{
        // Convert KVMessage to JSON String
        String msgAsString = msg.serialize();
        logger.info("SEND MESSAGE: " + msgAsString);
        output.println(msgAsString);
    }

    public void gracefulClose() {
        System.out.printf("THREAD-%s: Gracefully close detected, closing...\n", getId());
        isOpen = false;
    }

    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Receive a string message from socket and deserialize into Message object
     * @return Message received through socket
     * @throws IOException
     */
    private KVMessage receiveMessage() throws IOException {
        KVMessage msg = null;
        String msgString = null;
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        msgString = reader.readLine();
        if (msgString != null) {
            try {
                System.out.println(msgString);
                msg = objectMapper.readValue(msgString, Message.class);
            }
            catch (IOException e){
                System.out.println("readValue casued IO expcetion");
            }

        }
        return msg;
    }

    private UnifiedMessage receiveMessage2() throws IOException {
        UnifiedMessage msg = null;
        String msgString = null;
        msgString = TCPSockModule.recv(input);
        if (Objects.nonNull(msgString) && !msgString.equals("")) {
            try {
                logger.info("Received message: " + msgString);
                msg = new UnifiedMessage().deserialize(msgString);
                logger.info(String.format("Deserialized message: messageType=%s, statusType=%s",
                    msg.getMessageType(), msg.getStatusType()));
            }
            catch (Exception e){
                logger.error("readValue caused IO exception: " + e.getMessage());
            }

        }
        return msg;
    }
}