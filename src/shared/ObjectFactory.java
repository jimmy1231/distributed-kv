package shared;

import client.IClient;
import server.IServer;
import client.Client;
import server.Server;

public final class ObjectFactory {
	/*
	 * Creates a KVClient object for auto-testing purposes
	 */
    public static IClient createKVClientObject() {
    	return new Client();
    }
    
    /*
     * Creates a KVServer object for auto-testing purposes
     */
	public static IServer createKVServerObject(int port, int cacheSize, String strategy) {
		return new Server(port, cacheSize, strategy);
	}
}