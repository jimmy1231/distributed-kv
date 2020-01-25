package app_kvServer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.MessageFormat;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {
    private Integer id;
    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private KVServer server;
    private BufferedReader input;
    private PrintWriter output;
    private ObjectMapper objectMapper;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(int id, Socket clientSocket, KVServer server) {
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
            input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            objectMapper = new ObjectMapper();

            String connectionAck = "Connection to KVServer established: "
                                  + clientSocket.getLocalAddress() + " / "
                                  + clientSocket.getLocalPort();
            output.println(connectionAck);

            while(isOpen) {
                try {
                    KVMessage lastMsg = receiveMessage();
                    if (lastMsg != null){
                        KVMessage response = handleMessage(lastMsg);
                        sendMessage(response);
                    }

                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!"); //This message gets printed out when client disconnect
                    isOpen = false;

                } catch (Exception e) {
                    logger.error("Failed to handle the request and send the reply");
                }
            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);

        } finally {

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

    private KVMessage handleMessage(KVMessage msg) {
        KVMessage replyMsg = msg;

        try {
            if (msg.getStatus() == KVMessage.StatusType.PUT) {
                this.server.putKV(msg.getKey(), msg.getValue());
            }
        }
        catch (Exception e){

        }

        try{
            if (msg.getStatus() == KVMessage.StatusType.GET){
                String value = this.server.getKV(msg.getKey());
                replyMsg.setStatus(KVMessage.StatusType.GET_SUCCESS);
                replyMsg.setValue(value);
            }
        }
        catch (Exception e){
            replyMsg.setStatus(KVMessage.StatusType.GET_ERROR);
            logger.warn(MessageFormat.format("{0} Failed to find the value for key <{1}>",
                    msg.getStatus(),
                    msg.getKey()));
        }

        return replyMsg;
    }

    /**
     * Method sends a Message using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(KVMessage msg) throws Exception{
        // Convert KVMessage to JSON String
        String msgAsString = objectMapper.writeValueAsString(msg);
        output.println(msgAsString);
    }

    /**
     * Receive a string message from socket and deserialize into Message object
     * @return Message received through socket
     * @throws IOException
     */
    private KVMessage receiveMessage() throws IOException {
        KVMessage msg = null;
        String msgString = null;
        msgString = input.readLine();

        if (msgString != null) {
            try {
                msg = objectMapper.readValue(msgString, Message.class);
            }
            catch (IOException e){
                System.out.print("readValue casued IO expcetion");
            }

        }
        return msg;
    }
}