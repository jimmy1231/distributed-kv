package app_kvECS;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.function.Predicate;

import app_kvECS.impl.KVServerMetadataImpl;

import app_kvECS.impl.HashRingImpl;

import ecs.ECSNode;
import ecs.IECSNode;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.MessageType;
import shared.messages.UnifiedMessage;


public class ECSClient implements IECSClient {
    private static Logger logger = Logger.getLogger(ECSClient.class);
    private static final String CONFIG_DIR_PATH = "./src/app_kvECS/config/";
    private static final String KVSERVER_START_FILE = "./run_kvserver.sh";
    private static final String ECS_CONFIG_FILE = CONFIG_DIR_PATH + "ecs.config";
    private int poolSize; // max number of servers that can participate in the service
    private List<ECSNode> allNodes = new ArrayList<>();
    int serverCacheSize = 50000;
    String serverCacheStrategy = "FIFO";

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
                poolSize++;
                /* Parse server data */
                serverData = line.split(" ");
                name = serverData[0];
                host = serverData[1];
                port = Integer.parseInt(serverData[2]);

                System.out.printf("ADD SERVER: %s | %s:%d\n", name, host, port);
                /* Create ECSNode and add to ring */
                node = new ECSNode(name,host, port);
                node.setEcsNodeFlag(IECSNode.ECSNodeFlag.IDLE);
                ring.addServer(node);
                allNodes.add(node);

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

        KVServerMetadata metadata = null;
        UnifiedMessage req = null;
        UnifiedMessage res;

        String host;
        int port;
        TCPSockModule socketModule;
        for (ECSNode server : servers) {
            host = server.getNodeHost();
            port = server.getNodePort();
            try {
                setServerStatus(server, requestType);
                metadata = new KVServerMetadataImpl(server.getNodeName(),
                        server.getNodeHost(), server.getEcsNodeFlag(), ring);

                req = new UnifiedMessage.Builder()
                        .withMessageType(MessageType.ECS_TO_SERVER)
                        .withStatusType(requestType)
                        .withMetadata(metadata)
                        .build();

                socketModule = new TCPSockModule(host, port);
                System.out.println("Created generic sockets module");
                res = socketModule.doRequest(req);
                socketModule.close();
            } catch (Exception ex) {
                System.out.printf("ERROR: Could not complete request for server - %s:%d\n", host, port);
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
        ECSNode NodeToAdd = null;
        TCPSockModule conn;

        for (int i = 0; i<allNodes.size(); i++) {
            ECSNode currNode = allNodes.get(i);
            System.out.println("ADD NODE: " + currNode.getUuid());
            if (currNode.getEcsNodeFlag() == IECSNode.ECSNodeFlag.IDLE) {
                ring.addServer(currNode);
                KVServerMetadata metadata = new KVServerMetadataImpl(currNode.getNodeName(),
                        currNode.getNodeHost(), IECSNode.ECSNodeFlag.STOP, ring); //update the state to stopped

                try {
                    conn = new TCPSockModule(currNode.getNodeHost(), currNode.getNodePort());

                    // prepare a message to server to make it call initKVServer()
                    UnifiedMessage initKVCall = new UnifiedMessage.Builder()
                            .withMessageType(MessageType.ECS_TO_SERVER)
                            .withStatusType(KVMessage.StatusType.SERVER_INIT)
                            .withCacheSize(cacheSize)
                            .withCacheStrategy(cacheStrategy)
                            .withMetadata(metadata)
                        .build();

                    conn.doRequest(initKVCall);
                    conn.close();

                    ECSNode succssorNode = ring.getSuccessorServer(currNode);
                    if (Objects.isNull(succssorNode)) {
                        NodeToAdd = currNode;
                        currNode.setEcsNodeFlag(IECSNode.ECSNodeFlag.IDLE_START);
                        ring.updateRing();
                        break;
                    }

                    System.out.println("SUCCESSOR NODE: " + succssorNode.getUuid());
                    conn = new TCPSockModule(succssorNode.getNodeHost(), succssorNode.getNodePort());

                    // prepare a message to server to make it call initKVServer()
                    UnifiedMessage lockUnlockWriteCall = new UnifiedMessage.Builder()
                            .withMessageType(MessageType.ECS_TO_SERVER)
                            .withStatusType(KVMessage.StatusType.SERVER_WRITE_LOCK)
                            .build();

                    UnifiedMessage moveDataCall = new UnifiedMessage.Builder()
                            .withMessageType(MessageType.ECS_TO_SERVER)
                            .withStatusType(KVMessage.StatusType.SERVER_MOVEDATA)
                        .withKeyRange(ring.getServerHashRange(currNode).toArray())
                        .withServer(currNode)
                        .build();

                    lockUnlockWriteCall.setStatusType(KVMessage.StatusType.SERVER_WRITE_UNLOCK);

                    conn.doRequest(lockUnlockWriteCall);
                    conn.doRequest(moveDataCall);
                    conn.doRequest(lockUnlockWriteCall);
                    conn.close();

                    broadcastMetaDataUpdates();
                    NodeToAdd = currNode;
                    currNode.setEcsNodeFlag(IECSNode.ECSNodeFlag.IDLE_START);
                    ring.updateRing();
                    break;
                }
                catch (Exception e){
                    System.out.println("Error while adding server: " + e.getMessage());
                    logger.error("Error while adding server " + currNode.getNodeHost());
                }
            }
        }
        return NodeToAdd;
    }

    public void broadcastMetaDataUpdates(){
        TCPSockModule socketModule;

        IECSNode.ECSNodeFlag status;
        for (ECSNode server : allNodes) {
            status = server.getEcsNodeFlag();
            if (status.equals(IECSNode.ECSNodeFlag.STOP)
                || status.equals(IECSNode.ECSNodeFlag.SHUT_DOWN)) {
                continue;
            }

            String host = server.getNodeHost();
            int port = server.getNodePort();
            try {
                socketModule = new TCPSockModule(host, port);
                System.out.println("Created generic sockets module");
                KVServerMetadata newMetaData = new KVServerMetadataImpl(server.getNodeName(), host,
                        server.getEcsNodeFlag(), ring);

                UnifiedMessage notification = new UnifiedMessage.Builder()
                        .withMessageType(MessageType.ECS_TO_SERVER)
                        .withStatusType(KVMessage.StatusType.SERVER_UPDATE)
                        .withMetadata(newMetaData)
                        .build();

                socketModule.doRequest(notification);
                socketModule.close();
            } catch (Exception ex) {
                System.out.printf("ERROR: Could not broadcast metadata update notification");
            }
        }
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        ArrayList<IECSNode> addedNodes = new ArrayList<>();
        IECSNode addedNode;

        int i;
        for (i=0; i<count; i++) {
            addedNode = addNode(cacheStrategy, cacheSize);
            if (Objects.nonNull(addedNode)) {
                addedNodes.add(addedNode);
            } else {
                break;
            }
        }

        System.out.printf("ADD_NODES: ADDED %d NODES\n", addedNodes.size());
        return addedNodes;
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

        for (String nodeName : nodeNames){
            ECSNode nodeToRemove = ring.getServerByName(nodeName);
            ECSNode successorNode = ring.getSuccessorServer(nodeToRemove); // get it before update the ring

            if (Objects.isNull(successorNode)) {
                System.out.printf("NODE %s is the ONLY NODE, CANNOT REMOVE\n", nodeName);
                return false;
            }

            ring.removeServer(nodeToRemove);
            ring.updateRing();

            try{
                // Lock the node to delete
                TCPSockModule conn1 = new TCPSockModule(nodeToRemove.getNodeHost(), nodeToRemove.getNodePort());
                UnifiedMessage removeNodeCalls = new UnifiedMessage.Builder()
                        .withMessageType(MessageType.ECS_TO_SERVER)
                        .withStatusType(KVMessage.StatusType.SERVER_WRITE_LOCK)
                        .build();
                UnifiedMessage resp = conn1.doRequest(removeNodeCalls);

                if (resp.getStatusType() != KVMessage.StatusType.SUCCESS){
                    conn1.close();
                    System.out.println("Could not get writer lock");
                    logger.error("Could not get writer lock for " + nodeToRemove.getNodeName() +"\n");
                    continue; // skip the rest of the process
                }

                // Make & Update successor with new metadata

                KVServerMetadata newMetadata = new KVServerMetadataImpl(successorNode.getNodeName(),
                        successorNode.getNodeHost(), successorNode.getEcsNodeFlag(), ring);


                UnifiedMessage metadataUpdateCall= new UnifiedMessage.Builder()
                        .withMessageType(MessageType.ECS_TO_SERVER)
                        .withStatusType(KVMessage.StatusType.SERVER_UPDATE)
                        .withMetadata(newMetadata)
                        .build();

                TCPSockModule conn2 = new TCPSockModule(successorNode.getNodeHost(), successorNode.getNodePort());
                conn2.doRequest(metadataUpdateCall);
                conn2.close();

                // Prepare the MoveData message
                removeNodeCalls.setStatusType(KVMessage.StatusType.SERVER_MOVEDATA);
                removeNodeCalls.setKeyRange(ring.getServerHashRange(nodeToRemove).toArray());
                removeNodeCalls.setServer(successorNode);

                // broadcast updates to all node
                broadcastMetaDataUpdates();
                removeNodeCalls.setStatusType(KVMessage.StatusType.SHUTDOWN); // don't need to worry about other fields already populated
                conn1.doRequest(removeNodeCalls);
                conn1.close();
            }
            catch (Exception e){
                System.out.println("Error occurred: " + e.getMessage());
                logger.error("Error occurred while removing nodes\n");
                return false;
            }
        }

        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return ring.getServers();
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return ring.getServerByHash(new HashRing.Hash(Key));
    }

    public void printRing() {
        ring.print();
    }

    public static void main(String[] args) {
        CLI app = new CLI();
        app.run();
    }
}
