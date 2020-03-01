package app_kvECS;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.function.Predicate;

import app_kvECS.impl.HashRingImpl;
import ecs.ECSNode;
import ecs.IECSNode;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

public class ECSClient implements IECSClient {
    private static Logger logger = Logger.getLogger(ECSClient.class);
    private static final String CONFIG_DIR_PATH = "./src/app_kvECS/config/";
    private static final String KVSERVER_START_FILE = "./run_kvserver.sh";
    private static final String ECS_CONFIG_FILE = CONFIG_DIR_PATH + "ecs.config";

    private HashRing ring;

    public ECSClient() {
        ring = new HashRingImpl();
        parseConfigFile();
    }

    /**
     * Parse ecs.config file, add all servers to HashRing data structure,
     * and start up servers using run_kvserver bash script
     */
    private void parseConfigFile() {
        try {
            FileReader fr = new FileReader(ECS_CONFIG_FILE);
            BufferedReader reader = new BufferedReader(fr);
            String line, name, host;
            int port;
            String[] serverData;
            ECSNode node;
            Process proc;
            Runtime run = Runtime.getRuntime();
            while ((line = reader.readLine()) != null) {
                /* Parse server data */
                serverData = line.split(" ");
                name = serverData[0];
                host = serverData[1];
                port = Integer.parseInt(serverData[2]);

                /* Create ECSNode and add to ring */
                node = new ECSNode(name,host, port);
                node.setEcsNodeFlag(IECSNode.ECSNodeFlag.IDLE);
                ring.addServer(node);

                /* Start server process */
                proc = run.exec(new String[] {KVSERVER_START_FILE, host, serverData[2]});
            }
            reader.close();
            fr.close();
        } catch (Exception ex) {
            logger.error("Error reading ecs.config " + ex.getMessage());
        }
    }

    private void setServerStatus(ECSNode server, KVMessage.StatusType requestType) {
        if (requestType.equals(KVMessage.StatusType.START)) {
            server.setEcsNodeFlag(IECSNode.ECSNodeFlag.START);
        }
        else if (requestType.equals(KVMessage.StatusType.STOP)) {
            server.setEcsNodeFlag(IECSNode.ECSNodeFlag.STOP);
        }
        else if (requestType.equals(KVMessage.StatusType.SHUTDOWN)) {
            server.setEcsNodeFlag(IECSNode.ECSNodeFlag.SHUT_DOWN);
        }
    }

    private boolean sendFilteredRequest(Predicate<ECSNode> filter, KVMessage.StatusType requestType) {
        boolean success = true;
        List<ECSNode> servers = ring.filterServer(filter);

        KVAdminRequest req = new KVAdminRequest(requestType);
        KVAdminResponse res;

        String host;
        int port;
        GenericSocketsModule<KVAdminRequest, KVAdminResponse> socketModule;
        for (ECSNode server : servers) {
            host = server.getNodeHost();
            port = server.getNodePort();
            try {
                socketModule = new GenericSocketsModule<KVAdminRequest, KVAdminResponse>(host, port);
                res = socketModule.doRequest(req, KVAdminResponse.class);
                setServerStatus(server, requestType);
            } catch (Exception ex) {
                System.out.format("ERROR: Could not complete request for server - %s:%d\n", host, port);
                success = false;
            }
        }
        return success;
    }

    @Override
    public boolean start() {
        Predicate<ECSNode> pred = new Predicate<ECSNode>() {
            @Override
            public boolean test(ECSNode ecsNode) {
                return IECSNode.ECSNodeFlag.IDLE_START.equals(ecsNode.getEcsNodeFlag()) ||
                        IECSNode.ECSNodeFlag.STOP.equals(ecsNode.getEcsNodeFlag());
            }
        };

        return sendFilteredRequest(pred, KVMessage.StatusType.START);
    }

    @Override
    public boolean stop() {
        Predicate<ECSNode> pred = new Predicate<ECSNode>() {
            @Override
            public boolean test(ECSNode ecsNode) {
                return IECSNode.ECSNodeFlag.START.equals(ecsNode.getEcsNodeFlag());
            }
        };

        return sendFilteredRequest(pred, KVMessage.StatusType.STOP);
    }

    @Override
    public boolean shutdown() {
        Predicate<ECSNode> pred = new Predicate<ECSNode>() {
            @Override
            public boolean test(ECSNode ecsNode) {
                return IECSNode.ECSNodeFlag.START.equals(ecsNode.getEcsNodeFlag()) ||
                        IECSNode.ECSNodeFlag.STOP.equals(ecsNode.getEcsNodeFlag());
            }
        };
        return sendFilteredRequest(pred, KVMessage.StatusType.SHUTDOWN);
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }



    public static void main(String[] args) {
        CLI app = new CLI();
        app.run();
    }
}
