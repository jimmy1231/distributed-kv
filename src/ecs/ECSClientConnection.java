package ecs;

import app_kvServer.KVServer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import shared.messages.ECSMessage;
import shared.messages.KVECSMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.MessageFormat;

public class ECSClientConnection {
	private String id;
	private static Logger logger = Logger.getRootLogger();

	private volatile boolean isOpen;
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
	public ECSClientConnection(String id, Socket clientSocket, KVServer server) {
		this.id = id;
		this.clientSocket = clientSocket;
		this.server = server;
		input = null;
		output = null;
		objectMapper = null;
		isOpen = true;
	}

	public boolean isOpen() {
		return isOpen;
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

			String connectionAck = "Connection to ECSNode established: "
					+ clientSocket.getLocalAddress() + " / "
					+ clientSocket.getLocalPort();
			output.println(connectionAck);

			while(isOpen) {
				try {
					KVECSMessage lastMsg = receiveMessage();
					if (lastMsg != null){
						KVECSMessage response = handleMessage(lastMsg);
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

	/**
	 * Receive a string message from socket and deserialize into Message object
	 * @return Message received through socket
	 * @throws IOException
	 */
	private KVECSMessage receiveMessage() throws IOException {
		KVECSMessage msg = null;
		String msgString = input.readLine();

		if (msgString != null) {
			try {
				msg = objectMapper.readValue(msgString, ECSMessage.class);
			}
			catch (IOException e){
				System.out.println("readValue caused IO expcetion");
			}

		}
		return msg;
	}

	/**
	 * Method sends a Message using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream
	 */
	public void sendMessage(KVECSMessage msg) throws Exception{
		// Convert KVECSMessage to JSON String
		String msgAsString = objectMapper.writeValueAsString(msg);
		output.println(msgAsString);
	}

	private KVECSMessage handleMessage(KVECSMessage msg) {
		return null;
	}
}
