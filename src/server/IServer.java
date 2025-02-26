package server;

import ecs.ServerMetadata;
import ecs.ECSNode;

public interface IServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    /**
     * Get the port number of the server
     * @return  port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * @return  hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * @return  cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();

    /**
     * Start the KVServer, all client requests and all ECS requests are processed.
     */
    public void start();

    /**
     * Stops the KVServer, all client requests are rejected and only ECS requests
     * are processed.
     */
    public void stop();

    /**
     * Lock the KVServer for write operations.
     */
    public void lockWrite();

    /**
     * Unlock the KVServer from write operations.
     */
    public void unLockWrite();

    /**
     * Transfer a subset (range) of the KVServer's data to another
     * KVServer (reallocation before removing this server or adding
     * a new KVServer to the ring); send a notification to the ECS
     * when data transfer is complete.
     *
     * Moves ALL objects that fall within this range to the other
     * server.
     *
     * @param range
     * @param server
     */
    public void moveData(String[] range, ECSNode server);

    /**
     * Update the metadata repository of this server.
     *
     * @param metadata
     */
    public void update(ServerMetadata metadata);

    public void initKVServer(ServerMetadata metadata, int cacheSize, String cacheStrategy);
}
