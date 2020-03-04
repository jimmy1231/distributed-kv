package app_kvECS;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

import app_kvECS.impl.KVServerMetadataImpl;

import app_kvECS.impl.HashRingImpl;

import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.messages.KVDataSet;
import shared.messages.KVMessage;
import shared.messages.MessageType;
import shared.messages.UnifiedMessage;


public class ECSClient implements IECSClient {
    private static Logger logger = LoggerFactory.getLogger(ECSClient.class);
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

                logger.info("ADD SERVER: {} | {}:{}\n", name, host, port);
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
                res = socketModule.doRequest(req);
                socketModule.close();
            } catch (Exception ex) {
                logger.error("ERROR: Could not complete request for server - {}:{}\n",
                    host, port, ex);
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
        final String PREFIX = "<ADD_NODE>: ";
        ECSNode nodeToAdd = ring.findServer(node ->
            node.getEcsNodeFlag().equals(IECSNode.ECSNodeFlag.IDLE)
        );
        if (Objects.isNull(nodeToAdd)) {
            return null;
        }

        ring.addServer(nodeToAdd);
        nodeToAdd.setEcsNodeFlag(IECSNode.ECSNodeFlag.IDLE_START);
        ring.updateRing();

        TCPSockModule newNodeConn;
        try {
            newNodeConn = new TCPSockModule(
                nodeToAdd.getNodeHost(), nodeToAdd.getNodePort()
            );
        } catch (Exception e) {
            logger.error("Failed to connect", e);
            return null;
        }

        UnifiedMessage initKVMessage;
        try {
            initKVMessage = new UnifiedMessage.Builder()
                .withMessageType(MessageType.ECS_TO_SERVER)
                .withStatusType(KVMessage.StatusType.SERVER_INIT)
                .withCacheSize(cacheSize)
                .withCacheStrategy(cacheStrategy)
                .withMetadata(new KVServerMetadataImpl(
                    nodeToAdd.getNodeName(),
                    nodeToAdd.getNodeHost(),
                    IECSNode.ECSNodeFlag.STOP,
                    ring
                ))
                .build();
            newNodeConn.doRequest(initKVMessage);
            logger.info("INITKV Success");
        } catch (Exception e) {
            logger.error("INITKV Failed", e);
            return null;
        } finally {
            newNodeConn.close();
        }

        ECSNode succssorNode;
        {
            succssorNode = ring.getSuccessorServer(nodeToAdd);
            if (Objects.isNull(succssorNode)) {
                logger.info("No successors, returning..");
                return nodeToAdd;
            }
        }

        TCPSockModule successorNodeConn;
        try {
            successorNodeConn = new TCPSockModule(
                succssorNode.getNodeHost(), succssorNode.getNodePort()
            );
        } catch (Exception e) {
            logger.error("Failed to connect", e);
            return null;
        }

        UnifiedMessage writeLockMessage;
        UnifiedMessage moveDataMessage;
        UnifiedMessage writeUnlockMessage;
        try {
            writeLockMessage = new UnifiedMessage.Builder()
                .withMessageType(MessageType.ECS_TO_SERVER)
                .withStatusType(KVMessage.StatusType.SERVER_WRITE_LOCK)
                .build();

            String[] hashRange = ring.getServerHashRange(nodeToAdd).toArray();
            moveDataMessage = new UnifiedMessage.Builder()
                .withMessageType(MessageType.ECS_TO_SERVER)
                .withStatusType(KVMessage.StatusType.SERVER_MOVEDATA)
                .withKeyRange(hashRange)
                .withServer(nodeToAdd)
                .build();
            writeUnlockMessage = new UnifiedMessage.Builder()
                .withMessageType(MessageType.ECS_TO_SERVER)
                .withStatusType(KVMessage.StatusType.SERVER_WRITE_UNLOCK)
                .build();

            successorNodeConn.doRequest(writeLockMessage);
            successorNodeConn.doRequest(moveDataMessage);
            successorNodeConn.doRequest(writeUnlockMessage);
        } catch (Exception e) {
            logger.error("Transfer data failed", e);
            return null;
        } finally {
            successorNodeConn.close();
        }

        broadcastMetaDataUpdates();
        return nodeToAdd;
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
                KVServerMetadata newMetaData = new KVServerMetadataImpl(
                    server.getNodeName(),
                    server.getNodeHost(),
                    server.getEcsNodeFlag(),
                    ring);

                UnifiedMessage notification = new UnifiedMessage.Builder()
                    .withMessageType(MessageType.ECS_TO_SERVER)
                    .withStatusType(KVMessage.StatusType.SERVER_UPDATE)
                    .withMetadata(newMetaData)
                    .build();

                socketModule.doRequest(notification);
                socketModule.close();
            } catch (Exception ex) {
                logger.error("ERROR: Could not broadcast metadata update notification", ex);
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

        logger.info("Added {} nodes", addedNodes.size());
        return addedNodes;
    }

    public KVDataSet getServerData(String serverName) {
        ECSNode node = ring.getServerByName(serverName);
        if (Objects.isNull(node)) {
            logger.info("SERVER: '{}' does not exist..", serverName);
        }

        TCPSockModule conn;
        try {
            conn = new TCPSockModule(
                node.getNodeHost(), node.getNodePort()
            );
        } catch (Exception e) {
            logger.error("Failed to connect", e);
            return null;
        }

        UnifiedMessage message, response;
        try {
            message = new UnifiedMessage.Builder()
                .withMessageType(MessageType.ECS_TO_SERVER)
                .withStatusType(KVMessage.StatusType.SERVER_DUMP_DATA)
                .build();
            response = conn.doRequest(message);
        } catch (Exception e) {
            logger.error("Failed to get data", e);
            return null;
        }

        return response.getDataSet();
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
                logger.info("Cannot remove {}: only node", nodeName);
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
                    logger.error("Could not get writer lock for {}",
                        nodeToRemove.getNodeName());
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
            catch (Exception e) {
                logger.error("Error occurred while removing nodes", e);
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
        try {
            new LogSetup("logs/ecs.log", Level.OFF);
            CLI app = new CLI();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
