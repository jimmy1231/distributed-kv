package client;

import com.fasterxml.jackson.databind.ObjectMapper;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

public class KVStore implements KVCommInterface {
	private String serverAddress;
	private int serverPort;
	private Socket clientSocket;
	private PrintWriter output;
	private Scanner input;

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
	}

	@Override
	public void connect() throws Exception {
		clientSocket = new Socket(this.serverAddress, this.serverPort);
		output = new PrintWriter(clientSocket.getOutputStream());
		input = new Scanner(clientSocket.getInputStream());
	}

	@Override
	public void disconnect() {
		try{
			if (clientSocket != null) {
				clientSocket.close();
				serverAddress = null;
				serverPort = -1;
				clientSocket = null;
				output = null;
				input = null;
			}
		}
		catch(IOException e){
			// TODO log as error
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
		ObjectMapper serializer = new ObjectMapper();
		String msgAsString = serializer.writeValueAsString(msg);
		output.print(msgAsString);
	}
}
