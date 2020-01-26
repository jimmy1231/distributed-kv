package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KVServer implements IKVServer {
	private static Logger logger = Logger.getRootLogger();
	private final HashMap <String, ClientConnection> connectionStatusTable = new HashMap<>();
	private ServerSocket listener;
	private DSCache cache;
	private int port;
	private volatile boolean running;
	private KVServerDaemon daemon;

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
		cache = new DSCache(cacheSize, strategy);
		this.port = port;
		running = false;
		listener = null;

//        daemon = new KVServerDaemon(this);
//        daemon.start();
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
		boolean instorage = Disk.inStorage(key);
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
	public KVMessage.StatusType putKVWithStatusCheck(String key, String value) throws Exception{
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

		return status;
	}
	private KVMessage.StatusType checkMessageFormat(String key, String value){
		KVMessage.StatusType status = null;
		int keyLength = key.getBytes().length;
		int valueLength = key.getBytes().length;
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
			cache.clearCache();
		} catch (Exception e) {
			// TODO: server log this
		}
	}

	@Override
    public void clearStorage(){
	    clearCache();
		Disk.clearStorage();
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
		synchronized (connectionStatusTable) {
			System.out.println("SERVER: closing connection with ID=" + connectionId);
			connectionStatusTable.remove(connectionId);
		}
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
				new KVServer(port, cacheSize, strategy).run();
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
