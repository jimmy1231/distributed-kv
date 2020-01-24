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

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private BufferedReader input;
    private PrintWriter output;
    private ObjectMapper objectMapper;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket) {
        this.clientSocket = clientSocket;
        input = null;
        output = null;
        objectMapper = null;
        this.isOpen = true;
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

            String firstMessage = "Connection to KVServer established: "
                                  + clientSocket.getLocalAddress() + " / "
                                  + clientSocket.getLocalPort();
            sendMessage(firstMessage);

            while(isOpen) {
                try {
                    KVMessage latestMsg = receiveMessage();

                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!");
                    isOpen = false;
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

    /**
     * Method sends a Message using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(KVMessage msg) throws Exception{
        // Convert KVMessage to JSON String
        String msgAsString = objectMapper.writeValueAsString(msg);
        output.print(msgAsString);
    }

    /**
     * function to use when a plain string message is being sent
     * @param msg message in string format
     * @throws IOException
     */
    private void sendMessage(String msg) throws IOException {
        output.println(msg);
    }

    /**
     * Receive a string message from socket and deserialize into Message object
     * @return Message received through socket
     * @throws IOException
     */
    private KVMessage receiveMessage() throws IOException {
        String MsgAsString = input.readLine();
        Message receivedMsg = objectMapper.readValue(MsgAsString, Message.class);
        return receivedMsg;
    }
}