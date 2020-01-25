package app_kvClient;

import client.CLI;
import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;

import java.io.IOException;

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

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            client.CLI app = new CLI();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
