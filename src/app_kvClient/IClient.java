package app_kvClient;

public interface IClient {
    /**
     * Creates a new connection to hostname:port
     * @throws Exception
     *      when a connection to the server can not be established
     */
    public void newConnection(String hostname, int port) throws Exception;

    /**
     * Get the current instance of the Store object
     * @return  instance of KVCommInterface
     */
    public KVCommInterface getStore();
}
