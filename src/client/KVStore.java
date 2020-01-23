package client;

import shared.messages.KVMessage;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;

public class KVStore implements KVCommInterface {
	private String serverAddress;
	private int serverPort;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;

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
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
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
			//log as error
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
