package app_kvServer;

import org.apache.log4j.Logger;
import server.ClientConnection;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServer implements IKVServer {
	private static Logger logger = Logger.getRootLogger();

	private ServerSocket serverSocket;
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
		serverSocket = null;
	}
	
	@Override
	public int getPort(){
		return port;
	}

	@Override
    public String getHostname(){
		return serverSocket.getInetAddress().getHostName();
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
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
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

		if(serverSocket != null) {
			while(running){
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection =
							new ClientConnection(client);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+  " on port " + client.getPort());
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
			serverSocket.close();
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
}
