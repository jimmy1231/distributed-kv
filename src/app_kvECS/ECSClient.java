package app_kvECS;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

import app_kvECS.impl.KVServerMetadataImpl;

import app_kvECS.impl.HashRingImpl;
import app_kvECS.HashRing.*;

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

import static shared.messages.KVMessage.StatusType.SERVER_REPLICATE;


public class ECSClient implements IECSClient {
    private static Logger logger = LoggerFactory.getLogger(ECSClient.class);
    private static final String CONFIG_DIR_PATH = "./src/app_kvECS/config/";
    private static final String KVSERVER_START_FILE = "./run_kvserver.sh";
    private static final String ECS_CONFIG_FILE = CONFIG_DIR_PATH + "ecs.config";
    private static final long HEARTBEAT_TIMEOUT = 1000*30; // 30 seconds
    private int poolSize; // max number of servers that can participate in the service
    private List<ECSNode> allNodes = new ArrayList<>();
    private int serverCacheSize = 50000;
    private String serverCacheStrategy = "FIFO";

    private HashRing ring;
    private HeartbeatMonitor heartbeatMonitor;

    public ECSClient() {
        ring = new HashRingImpl();
        heartbeatMonitor = new HeartbeatMonitor(this, HEARTBEAT_TIMEOUT);
        parseConfigFile();

        heartbeatMonitor.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            /*
             * Interrupt will wake heartbeat from sleep,
             * then heartbeat monitor will shutdown
             * automatically.
             */
            heartbeatMonitor.interrupt();
        }));
    }

    public void quit() {
        heartbeatMonitor.interrupt();
        /*
         * Add other ECSClient shutdown code here..
         */
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

                logger.info("ADD SERVER: {} | {}:{}", name, host, port);
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

    public boolean sendFilteredRequest(Predicate<ECSNode> filter, KVMessage.StatusType requestType) {
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
                metadata = new KVServerMetadataImpl(
                    server.getNodeName(),
                    server.getNodeHost(),
                    server.getEcsNodeFlag(),
                    ring);

                req = new UnifiedMessage.Builder()
                    .withMessageType(MessageType.ECS_TO_SERVER)
                    .withStatusType(requestType)
                    .withMetadata(metadata)
                    .build();

                socketModule = new TCPSockModule(host, port);
                res = socketModule.doRequest(req);
                socketModule.close();
            } catch (Exception ex) {
                logger.error("ERROR: Could not complete request for server - {}:{}",
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
                return IECSNode.ECSNodeFlag.IDLE_START.equals(ecsNode.getEcsNodeFlag());
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
        nodeToAdd.setCacheStrategy(cacheStrategy);
        nodeToAdd.setCacheSize(cacheSize);
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
            if (status.equals(IECSNode.ECSNodeFlag.SHUT_DOWN)
                || status.equals(IECSNode.ECSNodeFlag.IDLE)) {
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

            if (ring.getNumOfServers() < 4) {
                logger.info("Cannot remove {}: at least 3 servers need to be running", nodeName);
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

    public void recoverServers(List<ECSNode> failedServers) {
        // TODO: we should probably lock this..
        HashRange S_i_range, S_i_succ_range;
        ECSNode S_i_succ, S_i_pred;
        for (ECSNode S_i : failedServers) {
            logger.info("Recovering Server: {}", S_i.getUuid());
            List<ECSNode> replicas = ring.getReplicas(S_i);
            if (replicas.size() == 0) {
                logger.info("No replicas for Server: {}", S_i.getUuid());

                // Always do this..
                ring.removeServer(S_i);
                ring.updateRing();
                continue;
            }

            S_i_succ = ring.getSuccessorServer(S_i);
            S_i_pred = ring.getPredecessorServer(S_i);
            assert(Objects.nonNull(S_i_succ));
            assert(Objects.nonNull(S_i_pred));

            S_i_range = new HashRange(S_i.getNodeHashRange());
            S_i_succ_range = new HashRange(S_i_succ.getNodeHashRange());

            ECSNode S_n = ring.findServer(node ->
                node.getEcsNodeFlag().equals(IECSNode.ECSNodeFlag.IDLE)
            );
            if (Objects.isNull(S_n)) {
                /* No new nodes to add.. */
                logger.info("Could not find new node to add, " +
                    "try our best to recover data to successor " +
                    "node: {}->{}",
                    S_i.getUuid(), S_i_succ.getUuid());
                try {
                    ECSRequestsLib.availMoveReplicatedData(
                        replicas, S_i_succ, S_i.getNodeHashRange(), ring
                    );
                } catch (Exception e) {
                    logger.error("ERROR: recovery failed!" +
                        " Tried to recover replicated data to" +
                        " new node..", e);
                    continue;
                }

                // Always do this..
                ring.removeServer(S_i);
                ring.updateRing();
                continue;
            }

            S_n.setEcsNodeFlag(IECSNode.ECSNodeFlag.IDLE_START);
            ring.updateRing();
            S_n.setEcsNodeFlag(S_i.getEcsNodeFlag());
            Hash HS_n = new Hash(S_n.getUuid());
            try {
                ECSRequestsLib.initServer(S_n,
                    S_i.getCacheStrategy(), S_i.getCacheSize(),
                    new KVServerMetadataImpl(
                        S_n.getNodeName(),
                        S_n.getNodeHost(),
                        IECSNode.ECSNodeFlag.STOP,
                        ring
                    ));

                // Case 1:
                if (S_i_range.inRange(HS_n)) {
                    ECSRequestsLib.availMoveReplicatedData(
                        replicas, S_n, S_n.getNodeHashRange(), ring
                    );
                    ECSRequestsLib.availMoveReplicatedData(
                        replicas, S_i_succ, S_i.getNodeHashRange(), ring
                    );
                }
                // Case 2:
                else if (S_i_succ_range.inRange(HS_n)) {
                    ECSRequestsLib.availMoveReplicatedData(
                        replicas, S_n, S_i.getNodeHashRange(), ring
                    );
                    ECSRequestsLib.availMoveData(
                        ring.getSuccessorServer(S_n), S_n,
                        S_n.getNodeHashRange(), ring
                    );
                }
                // Case 3:
                else {
                    ECSRequestsLib.availMoveReplicatedData(
                        replicas, S_i_succ, S_i.getNodeHashRange(), ring
                    );
                    ECSRequestsLib.availMoveData(
                        ring.getSuccessorServer(S_n), S_n,
                        S_n.getNodeHashRange(), ring
                    );
                }

                ECSRequestsLib.updateServerReplicate(S_n,
                    new KVServerMetadataImpl(
                        S_n.getNodeName(),
                        S_n.getNodeHost(),
                        S_i.getEcsNodeFlag(),
                        ring
                    )
                );
            } catch (Exception e) {
                logger.error("ERROR: recovery failed!" +
                    " Tried to recover replicated data to" +
                    " new node..", e);
            } finally {
                // Always do this..
                ring.removeServer(S_i);
                ring.updateRing();
            }
        }

        /*
         * Broadcast all servers for them to replicate based on
         * the sent hash ring.
         */
        sendFilteredRequest(n -> {
                IECSNode.ECSNodeFlag flg = n.getEcsNodeFlag();
                return flg.equals(IECSNode.ECSNodeFlag.START)
                    || flg.equals(IECSNode.ECSNodeFlag.STOP)
                    || flg.equals(IECSNode.ECSNodeFlag.IDLE_START);
            },
            SERVER_REPLICATE
        );
    }

    public void printRing() {
        ring.print();
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/ecs.log", Level.INFO);
            CLI app = new CLI();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
