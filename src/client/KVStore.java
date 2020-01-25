package client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;

enum connectionStatus {CONNECTED, DISCONNECTED, CONNECTION_LOST};

public class KVStore implements KVCommInterface {
	private String serverAddress;
	private int serverPort;
	private Socket clientSocket;
	private PrintWriter output;
	private BufferedReader input;
	private ObjectMapper objectMapper;
	private static Logger logger = Logger.getRootLogger();
	private connectionStatus status;

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

	}

	@Override
	public void connect() throws Exception {
		try{
			clientSocket = new Socket(this.serverAddress, this.serverPort);
			output = new PrintWriter(clientSocket.getOutputStream());
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

	@Override
	public KVMessage put(String key, String value) throws Exception {
		KVMessage requestMsg = new Message(key, value, KVMessage.StatusType.PUT);
		sendMessage(requestMsg);
		return requestMsg;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		KVMessage requestMsg = new Message(key, null, KVMessage.StatusType.GET);
		sendMessage(requestMsg);
		return requestMsg;
	}

	/**
	 * Serialize the msg and send it over socket
	 */
	public void sendMessage(KVMessage msg) throws Exception{
		// Convert KVMessage to JSON String
		String msgAsString = objectMapper.writeValueAsString(msg);
		output.print(msgAsString);
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
