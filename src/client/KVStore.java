package client;

import ecs.ServerMetadata;
import ecs.TCPSockModule;
import server.dsmr.MapReduce;
import server.dsmr.ReduceOutput;
import server.dsmr.impl.Sort;
import com.fasterxml.jackson.databind.ObjectMapper;
import ecs.ECSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.Pair;
import shared.messages.*;
import ecs.HashRing;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.rmi.server.ServerNotActiveException;
import java.util.*;

enum connectionStatus {CONNECTED, DISCONNECTED, CONNECTION_LOST};

public class KVStore implements KVCommInterface {
	private static final Logger logger = LoggerFactory.getLogger(KVStore.class);

	private String serverAddress; // The user is expected to know at least one server
	private int serverPort;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private ObjectMapper objectMapper;
	private connectionStatus status;
	private HashRing recentHashring;
	private int TIMEOUT = 10 * 10000;

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
			clientSocket.setSoTimeout(TIMEOUT);
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			objectMapper = new ObjectMapper();

			// Read the acknowledgment message from the server and print it out
			String connectionAck = TCPSockModule.recv(input);
			System.out.println(connectionAck);

			status = connectionStatus.CONNECTED;
		}
		catch (ConnectException e){
			System.out.println("Error! Connection refused. Check if server is running");
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

		UnifiedMessage request = new UnifiedMessage.Builder()
			.withUUID(UUID.randomUUID())
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
				logger.info("NOT_RESPONSIBLE: HashRing is stale, updating..");
				ServerMetadata returnedMetadata = replyMsg.getMetadata();
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
		UnifiedMessage request = new UnifiedMessage.Builder()
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
				logger.info("NOT_RESPONSIBLE: HashRing is stale, updating..");
				ServerMetadata returnedMetadata = replyMsg.getMetadata();
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
	 * Performs MapReduce operation, choose an available server as master
	 * Returns result as the keys of Reduced objects.
	 *
	 * @param keys
	 * @return
	 * @throws Exception
	 */
	@Override
	public String[] mapReduce(MapReduce.Type mrType, String[] keys) throws Exception {
		switch (mrType) {
			case WORD_FREQ:
				clientMRWordFreq(keys);
				break;
			case SORT:
				clientMRSort(keys);
				break;
			case K_MEANS_CLUSTERING:
				clientMRKMeans(keys);
				break;
		}
		UnifiedMessage msg = new UnifiedMessage.Builder()
			.withMessageType(MessageType.CLIENT_TO_SERVER)
			.withStatusType(KVMessage.StatusType.MAP_REDUCE)
			.withMrType(mrType)
			.withKeys(keys)
			.build();

		KVMessage reply = null;
		try {
			sendMessage(msg);
			reply = receiveMessage();
		} catch (Exception e) {
			logger.error("Error performing MapReduce: {}", keys, e);
			return new String[0];
		}

		if (Objects.nonNull(reply) && reply instanceof UnifiedMessage) {
			return ((UnifiedMessage)reply).getKeys();
		}

		return new String[0];
	}

	private void clientMRWordFreq(String[] keys) {
		UnifiedMessage msg, result;
		msg = new UnifiedMessage.Builder()
			.withMessageType(MessageType.CLIENT_TO_SERVER)
			.withStatusType(KVMessage.StatusType.MAP_REDUCE)
			.withMrType(MapReduce.Type.WORD_FREQ)
			.withKeys(keys)
			.build();

		long startTime, endTime;
		Pair<MRReport, List<ReduceOutput>> resultPair;
		List<ReduceOutput> outputs;
		MRReport mrReport;
		try {
			startTime = System.currentTimeMillis();
			resultPair = mrRequest(msg);
			endTime = System.currentTimeMillis();

			outputs = resultPair.getValue();
			mrReport = resultPair.getKey();
		} catch (Exception e) {
			return;
		}

		// Display N x COL_WIDTH table
		final int COL_WIDTH = 5;
		StringBuilder SB = new StringBuilder();
		List<Pair<String, String>> entries;
		int cnt = 0;
		int outputSize = 0;
		for (ReduceOutput r : outputs) {
			entries = r.getDataSet().getEntries();
			for (Pair<String, String> entry : entries) {
				if (cnt >= COL_WIDTH) {
					SB.append("\n");
					cnt = 0;
				}

				outputSize += entry.toString().length();
				SB.append(String.format(
					"%15s: %10s | ", entry.getKey(), entry.getValue())
				);

				cnt++;
			}
		}

		mrReport.setOutputSize(outputSize);
		String outputStr = SB.toString();
		logger.info(mrSummary(
			MapReduce.Type.WORD_FREQ,
			startTime, endTime, outputs.size(), keys.length, mrReport)
		+ "\n" + outputStr);
	}

	private void clientMRSort(String[] keys) {
		UnifiedMessage msg;
		msg = new UnifiedMessage.Builder()
			.withMessageType(MessageType.CLIENT_TO_SERVER)
			.withStatusType(KVMessage.StatusType.MAP_REDUCE)
			.withMrType(MapReduce.Type.SORT)
			.withKeys(keys)
			.build();

		long startTime, endTime;
		Pair<MRReport, List<ReduceOutput>> resultPair;
		List<ReduceOutput> outputs;
		MRReport mrReport;
		try {
			startTime = System.currentTimeMillis();
			resultPair = mrRequest(msg);
			endTime = System.currentTimeMillis();

			outputs = resultPair.getValue();
			mrReport = resultPair.getKey();
		} catch (Exception e) {
			return;
		}

		final int MAX_NUM_COLS = 10;
		int outputSize = 0;
		StringBuilder SB = new StringBuilder();
		Iterator<Pair<String, String>> it;
		for (ReduceOutput output : outputs) {
			output.getDataSet().sort((c1,c2) -> {
				int k1 = Integer.parseInt(c1.getKey());
				int k2 = Integer.parseInt(c2.getKey());

				if (k1 < k2) return -1;
				else if (k1 > k2) return 1;
				return 0;
			});

			it = output.getDataSet().iterator();
			List<String> binValues;
			String strBinValues;
			Pair<String, String> pair;
			int col = 0;
			while (it.hasNext()) {
				pair = it.next();
				SB.append(String.format("\n<BIN %s>\n", pair.getKey()));

				strBinValues = pair.getValue();
				binValues = Arrays.asList(strBinValues.split(Sort.DELIMITER));

				for (String value : binValues) {
					if (col >= MAX_NUM_COLS) {
						SB.append("\n");
						col = 0;
					}

					outputSize += value.length();
					SB.append(String.format("%10s | ", value));
					col++;
				}
			}
		}

		mrReport.setOutputSize(outputSize);
		String outputStr = SB.toString();
		logger.info(mrSummary(
			MapReduce.Type.SORT,
			startTime, endTime, outputs.size(), keys.length, mrReport)
			+ "\n" + outputStr);
	}

	private void clientMRKMeans(String[] keys) {
		UnifiedMessage msg;
		msg = new UnifiedMessage.Builder()
			.withMessageType(MessageType.CLIENT_TO_SERVER)
			.withStatusType(KVMessage.StatusType.MAP_REDUCE)
			.withMrType(MapReduce.Type.K_MEANS_CLUSTERING)
			.withKeys(keys)
			.build();

		// Wrapper
		Pair<MRReport, List<ReduceOutput>> resultPair;
		List<ReduceOutput> result;
		try {
			resultPair = mrRequest(msg);
			result = resultPair.getValue();
		} catch (Exception e) {
			return;
		}
	}

	private Pair<MRReport, List<ReduceOutput>>
	mrRequest(UnifiedMessage msg) throws Exception {
		KVMessage reply = null;
		try {
			sendMessage(msg);
			reply = receiveMessage();
		} catch (Exception e) {
			logger.error("Request error: {}", e);
			throw e;
		}

		UnifiedMessage result;
		if (Objects.nonNull(reply) && reply instanceof UnifiedMessage) {
			result = (UnifiedMessage)reply;
		} else {
			throw new Exception("Unrecognized message format");
		}

		List<ReduceOutput> outputs = new ArrayList<>();
		String[] resultKeys = result.getKeys();
		ReduceOutput output;
		for (String key : resultKeys) {
			try {
				output = new ReduceOutput(get(key).getValue());
				outputs.add(output);
			} catch (Exception e) {
				/* Swallow */
			}
		}

		return new Pair<>(result.getMrReport(), outputs);
	}

	private static String mrSummary(MapReduce.Type mrType,
									long timeStart,
									long timeEnd,
									int numOutputFiles,
									int numInputFiles,
									MRReport mrReport) {
		StringBuilder sb = new StringBuilder();

		sb.append("\n******************** MapReduce Summary ********************\n");
		sb.append(String.format("MapReduce Function: %s\n", mrType));
		sb.append(String.format("Input Files: %d\n", numInputFiles));
		sb.append(String.format("Output Files: %d\n", numOutputFiles));
		sb.append(String.format("Input size (bytes): %,d\n", mrReport.getInputSize()));
		sb.append(String.format("Output size (bytes): %,d\n", mrReport.getOutputSize()));
		sb.append(String.format("Total Available Workers: %d\n", mrReport.getNumAvailNodes()));
		sb.append(String.format("Map Workers: %d\n", mrReport.getNumMappers()));
		sb.append(String.format("Reduce Workers: %d\n", mrReport.getNumReducers()));
		sb.append(String.format("Time Elapsed (ms): %,d\n", timeEnd-timeStart));
		sb.append(String.format("Time Elapsed - MAP (ms): %,d\n", mrReport.getTimeMap()));
		sb.append(String.format("Time Elapsed - REDUCE (ms): %,d\n", mrReport.getTimeReduce()));
		sb.append("******************** MapReduce Summary ********************\n");

		return sb.toString();
	}

	@Override
	public void printRing() {
		if (Objects.nonNull(recentHashring)) {
			recentHashring.print();
		}
	}

	/**
	 * Serialize the msg and send it over socket
	 */
	private void sendMessage(UnifiedMessage msg) throws Exception {
		// Convert KVMessage to JSON String
		String msgAsString = msg.serialize();
		TCPSockModule.send(output, msgAsString);
		logger.info("addr {} port {}", this.clientSocket.getInetAddress(), this.clientSocket.getPort());
	}

	/**
	 * Receive a string message from socket and deserialize into Message object
	 * @return Message received through socket
	 * @throws IOException
	 */
	private KVMessage receiveMessage() throws IOException {
		UnifiedMessage msg = null;
		String msgString = null;
		msgString = TCPSockModule.recv(input);

		if (msgString != null) {
			try {
				msg = new UnifiedMessage().deserialize(msgString);
			}
			catch (Exception e){
				System.out.print("Failed to read from the socket");
				logger.warn("Failed to convert byte stream to Message object");
			}
		}
		return msg;
	}
}
