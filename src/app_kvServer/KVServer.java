package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServer implements IKVServer {
	private static Logger logger = Logger.getRootLogger();

	private ServerSocket listener;
	private DSCache cache;
	private int port;
	private boolean running;

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
	}
	
	@Override
	public int getPort(){
		return port;
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
		return Disk.inStorage(key);
	}

	@Override
    public boolean inCache(String key){
		return cache.inCache(key);
	}

	@Override
    public String getKV(String key) throws Exception {
		/* RETURNS NULL IF NOT FOUND */
		return cache.getKV(key);
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		cache.putKV(key, value);
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
					Socket communicationSocket = listener.accept();
					ClientConnection connection =
							new ClientConnection(communicationSocket);
					new Thread(connection).start();

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
		running = false;
		try {
			listener.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	@Override
    public void close(){
		// TODO: add more actions to perform
		kill();
	}

	/**
	 * Main entry point
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);

			//TODO: Add more args if necessary
			if(args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				new KVServer(port, 0, null).run();
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
