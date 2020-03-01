package client;

import app_kvECS.KVServerMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import ecs.ECSNode;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.Message;
import app_kvECS.HashRing;
import shared.messages.MessageType;
import shared.messages.UnifiedRequestResponse;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.rmi.server.ServerNotActiveException;

enum connectionStatus {CONNECTED, DISCONNECTED, CONNECTION_LOST};

public class KVStore implements KVCommInterface {
	private String serverAddress; // The user is expected to know at least one server
	private int serverPort;
	private Socket clientSocket;
	private PrintWriter output;
	private BufferedReader input;
	private ObjectMapper objectMapper;
	private static Logger logger = Logger.getRootLogger();
	private connectionStatus status;
	private HashRing recentHashring;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		serverAddress = address;
		serverPort = port;
		clientSocket = null;
		output = null;
		input = null;
		objectMapper = null;
		status = connectionStatus.DISCONNECTED;
		recentHashring = null;
	}

	@Override
	public void connect() throws Exception {
		try{
			clientSocket = new Socket(this.serverAddress, this.serverPort);
			output = new PrintWriter(clientSocket.getOutputStream(), true);
			input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			objectMapper = new ObjectMapper();

			// Read the acknowledgment message from the server and print it out
			String connectionAck = input.readLine();
			System.out.println(connectionAck);

			status = connectionStatus.CONNECTED;
		}
		catch (ConnectException e){
			System.out.println("Error! " +  "Connection refused. Check if server is running");
			logger.error("Could not establish connection!", e);
		}
	}

	@Override
	public void disconnect() {
		try{
			if (clientSocket != null) {
				input.close();
				output.close();
				clientSocket.close();

				input = null;
				output = null;
				clientSocket = null;

				objectMapper = null;
				serverAddress = null;
				serverPort = -1;
			}
		status = connectionStatus.DISCONNECTED;
		}
		catch(IOException e){
			// TODO log as error
		}
	}

	public boolean isConnectionAlive(){
		if (status == connectionStatus.CONNECTED){
			return true;
		}
		else{
			return false;
		}
	}

	private void changeConnection(String newAddress, int newPort){
		serverAddress = newAddress;
		serverPort = newPort;
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		boolean retransmit = true;

		UnifiedRequestResponse request = new UnifiedRequestResponse.Builder()
			.withMessageType(MessageType.CLIENT_TO_SERVER)
			.withKey(key)
			.withValue(value)
			.withStatusType(KVMessage.StatusType.PUT)
			.build();

		KVMessage replyMsg = null;

		while (retransmit) {
			//  Compute hash of the key -> determine which server to send to
			if (recentHashring != null) {
				ECSNode newServer = recentHashring.getServerByObjectKey(key);
				disconnect(); //disconnect from the original server
				changeConnection(newServer.getNodeHost(), newServer.getNodePort());
				connect(); // connect to the "correct" server
			}

			sendMessage(request);
			replyMsg = receiveMessage();

			// check if the message needs to be retransmitted
			if (replyMsg.getStatus() == Message.StatusType.SERVER_NOT_RESPONSIBLE) {
				KVServerMetadata returnedMetadata = replyMsg.getMetadata();
				recentHashring = returnedMetadata.getHashRing();
			}
			else if (replyMsg.getStatus() == Message.StatusType.SERVER_STOPPED) {
				throw new ServerNotActiveException();
			}
			else if (replyMsg.getStatus() == Message.StatusType.SERVER_WRITE_LOCK) {
				throw new ServerNotActiveException();
			}
			else {
				retransmit = false;
			}
		}
		return replyMsg;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		boolean retransmit = true;
		KVMessage replyMsg = null;
		UnifiedRequestResponse request = new UnifiedRequestResponse.Builder()
			.withMessageType(MessageType.CLIENT_TO_SERVER)
			.withKey(key)
			.withValue(null)
			.withStatusType(KVMessage.StatusType.GET)
			.build();

		while (retransmit) {
			//  Compute hash of the key -> determine which server to send to
			if (recentHashring != null) {
				ECSNode newServer = recentHashring.getServerByObjectKey(key);
				disconnect(); //disconnect from the original server
				changeConnection(newServer.getNodeHost(), newServer.getNodePort());
				connect(); // connect to the "correct" server
			}

			sendMessage(request);

			// Wait for the response from the server
			while (true){
				replyMsg = receiveMessage();
				if (replyMsg != null){
					break;
				}
			}

			// check if the message needs to be retransmitted
			if (replyMsg.getStatus() == Message.StatusType.SERVER_NOT_RESPONSIBLE) {
				KVServerMetadata returnedMetadata = replyMsg.getMetadata();
				recentHashring = returnedMetadata.getHashRing();
			}
			else if (replyMsg.getStatus() == Message.StatusType.SERVER_STOPPED) {
				throw new ServerNotActiveException();
			}
			else if (replyMsg.getStatus() == Message.StatusType.SERVER_WRITE_LOCK) {
				throw new ServerNotActiveException();
			}
			else {
				retransmit = false;
			}
		}
		return replyMsg;
	}

	/**
	 * Serialize the msg and send it over socket
	 */
	private void sendMessage(UnifiedRequestResponse msg) throws Exception {
		// Convert KVMessage to JSON String
		String msgAsString = msg.serialize();
		output.println(msgAsString);
	}

	/**
	 * Receive a string message from socket and deserialize into Message object
	 * @return Message received through socket
	 * @throws IOException
	 */
	private KVMessage receiveMessage() throws IOException {
		UnifiedRequestResponse msg = null;
		String msgString = null;
		msgString = input.readLine();

		if (msgString != null) {
			try {
				msg = new UnifiedRequestResponse().deserialize(msgString);
			}
			catch (Exception e){
				System.out.print("Failed to read from the socket");
				logger.warn("Failed to convert byte stream to Message object");
			}
		}
		return msg;
	}
}
