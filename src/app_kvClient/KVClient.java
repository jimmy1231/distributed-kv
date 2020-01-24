package app_kvClient;

import client.KVCommInterface;
import client.KVStore;

public class KVClient implements IKVClient {
    private KVCommInterface KVStoreInstance;
    private boolean running = false;

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        KVStoreInstance = new KVStore(hostname, port);
        running = true;
        KVStoreInstance.connect();
    }

    @Override
    public KVCommInterface getStore(){
        return KVStoreInstance;
    }

    public boolean isRunning() {
        return running;
    }
}
