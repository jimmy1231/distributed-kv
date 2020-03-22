package app_kvServer;

import app_kvECS.TCPSockModule;
import app_kvECS.HashRing;
import app_kvECS.KVServerMetadata;
import app_kvECS.impl.KVServerMetadataImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import ecs.ECSNode;
import ecs.IECSNode;
import shared.Pair;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVDataSet;
import shared.messages.KVMessage;
import shared.messages.MessageType;
import shared.messages.UnifiedMessage;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class KVServer implements IKVServer {
	private static Logger logger = Logger.getRootLogger();
	private final ConcurrentHashMap <String, ClientConnection> connectionStatusTable = new ConcurrentHashMap<>();
	private ServerSocket listener;
	private DSCache cache;
	private int port;
	private volatile boolean running;
	private KVServerDaemon daemon;
	private KVServerMetadata metadata;
	private Disk disk;
	private ArrayList<Pair<UUID, KVMessage.StatusType>> primaryPutRequestList;
	private Map<String, Disk> replicatedDisks;
	private Map<String, ArrayList<Pair<UUID, KVMessage.StatusType>>> replicatedPutRequestList;
	private String[] replicas; // Name of ECS nodes that are replicas of this server

	class KVServerDaemon extends Thread {
		KVServer server;

		KVServerDaemon(KVServer server) {
			this.server = server;
		}

		@Override public void run() {
			server.run();
			System.out.println("KVServer Daemon Exit");
		}
	}

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		disk = new Disk(String.format("kv_store_%d.txt", port));
		replicas = new String[2];
		primaryPutRequestList = new ArrayList<Pair<UUID, KVMessage.StatusType>>();
		replicatedDisks = new HashMap<String, Disk>();
		replicatedPutRequestList = new HashMap<String, ArrayList<Pair<UUID, KVMessage.StatusType>>>();
		cache = new DSCache(cacheSize, strategy, disk);
		this.port = port;
		running = false;
		listener = null;
		metadata = new KVServerMetadataImpl(null, "localhost", IECSNode.ECSNodeFlag.IDLE, null);

        daemon = new KVServerDaemon(this);
        daemon.start();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					logger.info("sysexit detected, flushing cache");
					kill();
				} catch (Exception e) {
					logger.fatal("failed to flush cache on sysexit");
				}
			}
		});
	}

	public KVServerMetadata getMetdata() {
		return metadata;
	}

	@Override
	public int getPort(){
		return listener.getLocalPort();
	}

	@Override
    public String getHostname(){
		return listener.getInetAddress().getHostName();
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		return cache.getCacheStrategy();
	}

	@Override
    public int getCacheSize(){
		return cache.getCacheSize();
	}

	@Override
    public boolean inStorage(String key){
		boolean instorage = disk.inStorage(key);
		System.out.println("IN STORAGE? " + key + " " + instorage);
		return instorage;
	}

	@Override
    public boolean inCache(String key){
		boolean incache = cache.inCache(key);
		System.out.println("IN CACHE? " + key + " " + incache);
		return incache;
	}

	@Override
    public String getKV(String key) throws Exception {
		if (key.getBytes().length < 1){
			throw new Exception();
		}

		/* RETURNS NULL IF NOT FOUND */
		return cache.getKV(key);
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		System.out.printf("PUTKV->REFLECT: %s -> %s\n", key, value);
		try {
			cache.putKV(key, value);
		} catch (Exception e) {
			/* Swallow */
		}
	}

	public void putKVProd(String key, String value) throws Exception{
		System.out.printf("PUTKV->REFLECT: %s -> %s\n", key, value);
		cache.putKV(key, value);
	}

	/**
	 * Wrapper function for putKV. Because putKV given is a void function, status type cannot be determined in certain
	 * cases (for example, PUT_UPDATE). Thus, this wrapper function provides "return code" to the communication layer
	 * so that the server can send the client an appropriate message
	 * @param key
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public KVMessage.StatusType putKVWithStatusCheck(UUID uuid, String key, String value) throws Exception{
		// Check if the request with 'uuid' was processed last time
        // Even if it has been processed before, the last result was ERROR then try again
		if (primaryPutRequestList.get(0).getKey() == uuid
				&& primaryPutRequestList.get(0).getValue() != KVMessage.StatusType.PUT_ERROR){
			return primaryPutRequestList.get(0).getValue();
		}

		KVMessage.StatusType status = checkMessageFormat(key, value);

		// If format is invalid, just return ERROR status right away
		if (status != null){
			return status;
		}

		if (inStorage(key)){
			// key exists in the cache. Either PUT_UPDATE/ERROR or DELETE_SUCCESS/ERROR
			putKVProd(key, value);

			// Delete scenario
			if (value == null || value.equals("null") || value.equals("")) {
				status =  KVMessage.StatusType.DELETE_SUCCESS;
			}
			else {
				status = KVMessage.StatusType.PUT_UPDATE;
			}
		}
		else{ // fresh PUT case
			putKVProd(key, value);
			status = KVMessage.StatusType.PUT_SUCCESS;
		}

		if (Objects.nonNull(this.replicas)) {
			// Forward client's request to the replicas through socket message
			UnifiedMessage rsp1 = forwardRequestToReplica(this.replicas[0], key, value, KVMessage.StatusType.PUT);
			UnifiedMessage rsp2 = forwardRequestToReplica(this.replicas[1], key, value, KVMessage.StatusType.PUT);

			// Add to the head of the list (Index = 0 -> most recent request)
			primaryPutRequestList.add(0, new Pair<>(uuid, status));

			// when all acks are received, respond to the client
			if (status.equals(rsp1.getStatusType()) && status.equals(rsp2.getStatusType())) {
				logger.info("Returning " + status.toString() + "to the client");
				return status;
			} else {
				logger.error("Primary and replicas responses are not consistent, Return PUT_ERROR to the client");
				return KVMessage.StatusType.PUT_ERROR;
			}
		}

		else{
			return status;
		}
	}


	public UnifiedMessage requestReplicatedDisk() {
		this.forwardRequestToReplica(replicas[0], null, null, KVMessage.StatusType.SHOW_REPLICATION);
		this.forwardRequestToReplica(replicas[1], null, null, KVMessage.StatusType.SHOW_REPLICATION);

		return null; // Just dummy return val since socket call blocks until it receives response
	}

	public void printReplicatedDisk(ECSNode primary){
		Disk disk = this.replicatedDisks.get(primary.getNodeName());
		logger.info(disk.getAll().toString());
	}

	public KVMessage.StatusType replicate(String coordinatorName, UUID uuid, String key, String value){
	    // Check if this replicate request has been processed the last time
	    ArrayList<Pair<UUID, KVMessage.StatusType>> putRequestList = this.replicatedPutRequestList.get(coordinatorName);
        Pair<UUID, KVMessage.StatusType> mostRecentRequest = putRequestList.get(0);

        if (mostRecentRequest.getKey() == uuid
                && mostRecentRequest.getValue() != KVMessage.StatusType.PUT_ERROR){
            return mostRecentRequest.getValue();
        }

		Disk disk = replicatedDisks.get(coordinatorName);
		KVMessage.StatusType status;

		if (disk != null){
			if (disk.inStorage(key)){
				// key exists in the cache. Either PUT_UPDATE/ERROR or DELETE_SUCCESS/ERROR
				disk.putKV(key, value);

				// Delete scenario
				if (value == null || value.equals("null") || value.equals("")) {
					status =  KVMessage.StatusType.DELETE_SUCCESS;
				}
				else {
					status = KVMessage.StatusType.PUT_UPDATE;
				}
			}
			else{ // fresh PUT case
				disk.putKV(key, value);
				status = KVMessage.StatusType.PUT_SUCCESS;
			}
			disk.putKV(key, value);
			putRequestList.add(0, new Pair<>(uuid, status));

			return status;
		}
		else{
			throw new NullPointerException("No disk exists for " + coordinatorName);
		}
	}

	/**
	 * Update Node names of the current Server's replicas using the current metadata available
	 */
	private void updateReplicas(){
		HashRing ring = metadata.getHashRing();
		String myNodeName = metadata.getName();
		ECSNode myNode = ring.getServerByName(myNodeName);
		ECSNode succNode1 = ring.getSuccessorServer(myNode); // this throws an error
		ECSNode succNode2 = ring.getSuccessorServer(succNode1);

		// sanity check
		assert (Objects.nonNull(succNode1) && Objects.nonNull(succNode2));

		String rep1 = succNode1.getNodeName();
		String rep2 = succNode2.getNodeName();

		this.replicas[0] = rep1;
		this.replicas[1] = rep2;
	}

	/**
	 * Each server needs to keep 2 sets of replicated data
	 * For example, if there are three servers, Server1, Server2 and Sever3, in the hash ring,
	 * then Server 3 needs to have a copy of Server1's (coordinator 2) and Server2's (coordinator 1) data
	 */
	private void initReplicatedDisks(){
		ECSNode[] replicas = new ECSNode[2];
		HashRing ring = metadata.getHashRing();
		String myNodeName = metadata.getName();
		ECSNode myNode = ring.getServerByName(myNodeName);
		ECSNode coordinator1 = ring.getPredecessorServer(myNode); // this throws an error
		ECSNode coordinator2 = ring.getPredecessorServer(coordinator1);

		// sanity check
		assert (Objects.nonNull(coordinator1) && Objects.nonNull(coordinator2));

		String coordName1 = coordinator1.getNodeName();
		String coordName2 = coordinator2.getNodeName();

		this.replicatedDisks.put(coordName1,
				new Disk(String.format("kv_store_%d.txt", coordinator1.getNodePort())));
		this.replicatedDisks.put(coordName2,
				new Disk(String.format("kv_store_%d.txt", coordinator2.getNodePort())));

		// Also initialize putRequestList to keep track of replication requests
		this.replicatedPutRequestList.put(coordName1, new ArrayList<>());
		this.replicatedPutRequestList.put(coordName2, new ArrayList<>());
	}

	/**
	 * As a primary (coordinator) send a PUT request to the replicas
	 * It is a blocking call due to how TCPSockModule is implemented
	 * @param replicaName Node name of replica who is receiving the forwarded request
	 * @param key key to store
	 * @param value value to store
	 * @return Response message from the replica
	 */
	private UnifiedMessage forwardRequestToReplica(String replicaName, String key, String value,
												   KVMessage.StatusType type){
		int TIMEOUT = 10 * 1000;
		HashRing ring = this.metadata.getHashRing();
		ECSNode myNode = ring.getServerByName(this.metadata.getName());
		ECSNode replica = ring.getServerByName(replicaName);
		TCPSockModule module = null;
		UnifiedMessage req, resp;
		try {
			// Sending message to the replica servers
			module = new TCPSockModule(replica.getNodeHost(), replica.getNodePort(), TIMEOUT);

			req = new UnifiedMessage.Builder()
					.withMessageType(MessageType.SERVER_TO_SERVER)
					.withStatusType(type)
					.withKey(key)
					.withValue(value)
					.build();

			resp = module.doRequest(req);
			logger.info("Successfully forwarded request to replica " + replicaName);
			return resp;

		} catch (Exception e) {
			logger.error(String.format(
					"Unable to forward request: %s",
					e.getMessage()), e);

			KVMessage.StatusType respType = null;
			if (type == KVMessage.StatusType.PUT){
				respType = KVMessage.StatusType.PUT_ERROR;
			}
			else if (type == KVMessage.StatusType.SHOW_REPLICATION){
				respType = KVMessage.StatusType.ERROR;
			}

			resp = new UnifiedMessage.Builder()
					.withMessageType(MessageType.SERVER_TO_SERVER)
					.withStatusType(respType)
					.withPrimary(myNode) // This is the sender node (indicate who primary server is)
					.withKey(key)
					.withValue(value)
					.build();

			return resp;

		} finally {
			if (Objects.nonNull(module)) {
				module.close();
			}
		}
	}

	private KVMessage.StatusType checkMessageFormat(String key, String value){
		KVMessage.StatusType status = null;
		int keyLength = key.getBytes().length;
		int valueLength = value.getBytes().length;
		int KEY_MAXSIZE = 20; // in bytes
		int VALUE_MAXSIZE = 120000; // in bytes


		// Value exceeded 120KB - can't be delete request
		if (valueLength >= VALUE_MAXSIZE){
			status = KVMessage.StatusType.PUT_ERROR;
			String errorMsg = MessageFormat.format("Maximum size of value (120KB) exceeded. {0}",
					status);
			logger.error(errorMsg);
			System.out.print(errorMsg);
		}
		// check for empty key or max-exceeding key
		else if (keyLength >= KEY_MAXSIZE || keyLength < 1) {
			if (value == null || value.equals("null") || value.equals("")) {
				status = KVMessage.StatusType.DELETE_ERROR;
			} else {
				status = KVMessage.StatusType.PUT_ERROR;
			}
			String errorMsg = MessageFormat.format("Maximum size of key (20 Bytes) exceeded. {0}",
					status);
			logger.error(errorMsg);
			System.out.print(errorMsg);
		}

		return status;
	}
	@Override
    public void clearCache(){
		try {
			cache.clearCache(false);
		} catch (Exception e) {
			// TODO: server log this
		}
	}

	@Override
    public void clearStorage(){
	    clearCache();
		disk.clearStorage();
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			listener = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ listener.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	public void run() {
		running = initializeServer();

		if(listener != null) {
			while(running){
				try {
					String connectionId = UUID.randomUUID().toString();
					Socket communicationSocket = listener.accept();
					ClientConnection connection = new ClientConnection(
							connectionId,
							communicationSocket,
							this);

					connectionStatusTable.put(connectionId, connection);
					connection.start();

					logger.info("Connected to "
							+ communicationSocket.getInetAddress().getHostName()
							+  " on port " + communicationSocket.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	@Override
    public void kill(){
		System.out.println("CALLING KILL!!!!!!");
		/*
		 * (1) Terminate all connections immediately
		 */
		running = false;
		try {
			for (ClientConnection conn : connectionStatusTable.values()) {
				System.out.println("Kill");
				conn.gracefulClose();
			}

			listener.close();
//			daemon.stop();
			logger.info("Daemon thread exited");
        } catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	public void closeConnection(String connectionId) {
			System.out.println("SERVER: closing connection with ID=" + connectionId);
			connectionStatusTable.remove(connectionId);
	}

	@Override
    public void close(){
		System.out.println("CALLING CLOSE!!!!!!");
		running = false;
		try {
			for (ClientConnection conn : connectionStatusTable.values()) {
				System.out.println("Graceful close: THREAD_ID=" + conn.getId());
				if (conn.isOpen()) {
					conn.gracefulClose();
				}
			}

			connectionStatusTable.clear();
			listener.close();
//			daemon.stop();
		} catch (IOException e) {
			logger.error("Error! " +
				"Unable to close socket on port: " + port, e);
		}
	}

	public void start() {
		if (metadata == null)
			metadata = new KVServerMetadataImpl(null, "localhost", IECSNode.ECSNodeFlag.IDLE, null);

		metadata.setECSNodeFlag(IECSNode.ECSNodeFlag.START);
	}

	public void stop() {
		if (metadata == null)
			metadata = new KVServerMetadataImpl(null, "localhost", IECSNode.ECSNodeFlag.IDLE, null);

		metadata.setECSNodeFlag(IECSNode.ECSNodeFlag.STOP);
	}

	public void shutdown() {
		this.clearStorage();
		this.kill();
	}

	/**
	 * Lock the KVServer for write operations.
	 */
	@Override
	public void lockWrite() {
		metadata.setECSNodeFlag(IECSNode.ECSNodeFlag.KV_TRANSFER);
	}

	/**
	 * Unlock the KVServer from write operations.
	 */
	@Override
	public void unLockWrite() {
		IECSNode.ECSNodeFlag flag = metadata.getEcsNodeFlag();
		if (flag.equals(IECSNode.ECSNodeFlag.KV_TRANSFER)) {
			metadata.setECSNodeFlag(IECSNode.ECSNodeFlag.START);
		}
	}

	/**
	 * Transfer a subset (range) of the KVServer's data to another
	 * KVServer (reallocation before removing this server or adding
	 * a new KVServer to the ring); send a notification to the ECS
	 * when data transfer is complete.
	 * <p>
	 * Moves ALL objects that fall within this range to the other
	 * server.
	 *
	 * @param range
	 * @param server
	 */
	@Override
	public void moveData(String[] range, ECSNode server) {
		HashRing.HashRange hashRange = new HashRing.HashRange(range);

		List<Pair<String, String>> entries = new ArrayList<>();
		try {
			entries = cache.findAndRemove(
				new Predicate<Pair<String, String>>() {
					@Override
					public boolean test(Pair<String, String> entry) {
						return hashRange.inRange(
							new HashRing.Hash(entry.getKey())
						);
					}
				}
			);
		} catch (Exception e) {
			logger.error(String.format(
				"Error MoveData: %s", e.getMessage()), e);
		}

		/*
		 * Move data to another server via socket request
		 */
		TCPSockModule module = null;
		UnifiedMessage req, resp;
		try {
			module = new TCPSockModule(
				server.getNodeHost(), server.getNodePort()
			);

			KVDataSet dataSet = new KVDataSet(entries);
			logger.info("DATA SET TO TRANSFER: " + dataSet.serialize());
			req = new UnifiedMessage.Builder()
				.withMessageType(MessageType.SERVER_TO_SERVER)
				.withStatusType(KVMessage.StatusType.SERVER_MOVEDATA)
				.withDataSet(dataSet)
				.build();

			resp = module.doRequest(req);
			logger.info("DATA TRANSFER COMPLETE: " + resp.serialize());
		} catch (Exception e) {
			logger.error(String.format(
				"Unable to send MoveData request: %s",
				e.getMessage()), e);
		} finally {
			if (Objects.nonNull(module)) {
				module.close();
			}
		}
	}

	public void recvData(KVDataSet dataSet) {
		List<Pair<String, String>> entries = dataSet.getEntries();
		try {
			for (Pair<String, String> entry : entries) {
				cache.putKV(entry.getKey(), entry.getValue());
			}
		} catch (Exception e) {
			logger.error(String.format(
				"Unable to transfer data: %s. Data: %s",
				e.getMessage(), dataSet.serialize()
			), e);
		}
	}

	public IECSNode.ECSNodeFlag getStatus() {
		return metadata.getEcsNodeFlag();
	}

	/**
	 * Update the metadata repository of this server.
	 *
	 * @param metadata
	 */
	@Override
	public void update(KVServerMetadata metadata) {
		this.metadata = metadata;
	}

	public KVDataSet getAllData() {
		List<Pair<String, String>> entries = disk.getAll();
		KVDataSet dataSet = new KVDataSet(entries);
		logger.info("GET ALL DATA: " + dataSet.serialize());
		return dataSet;
	}

	@Override
	public void initKVServer(KVServerMetadata metadata, int cacheSize, String cacheStrategy) {
		this.update(metadata);
		this.cache = new DSCache(cacheSize, cacheStrategy, disk);
		logger.info("Updated metadata");
		//if (metadata.getHashRing().getNumOfServers() >= 3) {
	       	//	logger.info("More than 3 servers exist. start replication");
		//	this.updateReplicas();
		//	this.initReplicatedDisks();
		//}
		//else{
		//	logger.info("Less than 3 servers!");
		//}
	}

	/**
	 * Main entry point
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);

			if(args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <cache size> <caching strategy>!");
			} else {
				int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                String strategy = args[2];
				new KVServer(port, cacheSize, strategy);
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);

		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}

}
