package app_kvClient;

import client.KVCommInterface;
import client.KVStore;

public class KVClient implements IKVClient {
    @Override
    public void newConnection(String hostname, int port) throws Exception{
        KVStore KVStoreInstance = new KVStore(hostname, port);
        KVStoreInstance.connect();
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return null;
    }
}
