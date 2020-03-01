package app_kvServer;

import app_kvECS.GenericSocketsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import ecs.IECSNode;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.UnifiedRequestResponse;

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
                    UnifiedRequestResponse request = receiveMessage2();
                    if (Objects.isNull(request)) {
                        continue;
                    }

                    UnifiedRequestResponse response;
                    switch(request.getMessageType()) {
                        case CLIENT_TO_SERVER:
                        case SERVER_TO_CLIENT:
                            response = handleMessage(request);
                            break;
                        case ECS_TO_SERVER:
                        case SERVER_TO_ECES:
                            response = handleAdminMessage(request);
                            break;
                        default:
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

    private UnifiedRequestResponse handleMessage(UnifiedRequestResponse msg) {
        String key = msg.getKey();
        String value = msg.getValue();
        KVMessage.StatusType status = null;
        UnifiedRequestResponse replyMsg = msg;
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

    private UnifiedRequestResponse handleAdminMessage(UnifiedRequestResponse msg) {
        UnifiedRequestResponse replyMsg = msg;
        KVMessage.StatusType status = msg.getStatusType();
        System.out.println(status);
        if (KVMessage.StatusType.START.equals(status)) {
            server.start();
            status = KVMessage.StatusType.SUCCESS;
        }
        else if (KVMessage.StatusType.STOP.equals(status)) {
            server.stop();
            status = KVMessage.StatusType.SUCCESS;
        }
        else if (KVMessage.StatusType.SHUTDOWN.equals(status)) {
            server.shutdown();
            status = KVMessage.StatusType.SUCCESS;
        }

        replyMsg.setStatusType(status);
        return replyMsg;
    }

    /**
     * Method sends a Message using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(UnifiedRequestResponse msg) throws Exception{
        // Convert KVMessage to JSON String
        String msgAsString = msg.serialize();
        System.out.println(msgAsString);
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

    private UnifiedRequestResponse receiveMessage2() throws IOException {
        UnifiedRequestResponse msg = null;
        String msgString = null;
        msgString = GenericSocketsModule.recv(input);
        if (msgString != null) {
            try {
                logger.info(msgString);
                msg = new UnifiedRequestResponse().deserialize(msgString);
            }
            catch (Exception e){
                logger.error("readValue casued IO expcetion", e);
            }

        }
        return msg;
    }
}