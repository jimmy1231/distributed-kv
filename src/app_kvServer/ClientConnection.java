package app_kvServer;

import app_kvECS.HashRing;
import app_kvECS.TCPSockModule;
import app_kvECS.KVServerMetadata;
import app_kvServer.dsmr.*;
import app_kvServer.dsmr.impl.KMeansClustering;
import app_kvServer.dsmr.impl.Sort;
import app_kvServer.dsmr.impl.WordFreq;
import com.fasterxml.jackson.databind.ObjectMapper;
import ecs.ECSNode;
import ecs.IECSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.Pair;
import shared.messages.*;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static shared.messages.KVMessage.StatusType.*;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection extends Thread {
    private static Logger logger = LoggerFactory.getLogger(ClientConnection.class);
    private String id;

    private volatile boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private KVServer server;
    private InputStream input;
    private OutputStream output;
    private ObjectMapper objectMapper;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(String id, Socket clientSocket, KVServer server) {
        this.id = id;
        this.clientSocket = clientSocket;
        this.server = server;
        input = null;
        output = null;
        objectMapper = null;
        isOpen = true;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
            objectMapper = new ObjectMapper();

            TCPSockModule.send(output, String.format(
                "Connection to KVServer established: %s:%s",
                clientSocket.getLocalAddress(),
                clientSocket.getLocalPort()
            ));

            while(isOpen) {
                try {
                    UnifiedMessage request = receiveMessage2();
                    if (Objects.isNull(request)) {
                        continue;
                    }

                    UnifiedMessage response;
                    switch(request.getMessageType()) {
                        case CLIENT_TO_SERVER:
                        case SERVER_TO_CLIENT:
                            logger.info("CLIENT request: {}", request.toString());
                            response = handleClientMessage(request);
                            break;
                        case ECS_TO_SERVER:
                        case SERVER_TO_ECS:
                            logger.info("ECS request: {}", request.toString());
                            response = handleAdminMessage(request);
                            break;
                        case SERVER_TO_SERVER:
                            logger.info("SERVER request: {}", request.toString());
                            response = handleServerMessage(request);
                            break;
                        default:
                            logger.info("UNKNOWN request: {}", request.toString());
                            throw new Exception("Invalid message type");
                    }

                    sendMessage(response);
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!"); //This message gets printed out when client disconnect
                    isOpen = false;
                } catch (Exception e) {
                    logger.error("Failed to handle the request and send the reply: ", e);
                }
            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);

        } finally {
            System.out.printf("CLOSING THREAD=%s\n", getId());
            server.closeConnection(id);
            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    objectMapper = null;
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    private UnifiedMessage handleClientMessage(UnifiedMessage msg) {
        IECSNode.ECSNodeFlag flg = server.getStatus();
        if (!flg.equals(IECSNode.ECSNodeFlag.START)) {
            logger.info("SERVER: {}. Not accepting client requests", flg);
            msg.setStatusType(KVMessage.StatusType.SERVER_STOPPED);
            return msg;
        }

        UnifiedMessage replyMsg = msg;

        /* Special case: MapReduce request - Master */
        if (msg.getStatusType().equals(MAP_REDUCE)) {
            return handleMRMaster(msg.getMrType(), msg.getKeys());
        }

        /*
         * Check if the key requested is in the server's hash range.
         * If it is in range, process as normal, if it isn't, reply
         * SERVER_NOT_RESPONSIBLE to the client along with the updated
         * HashRing (embedded in metadata).
         *
         * Since the server has the latest version of metadata,
         * the client should, upon receiving the reply, be able to
         * (1) reconnect, and (2) retry the request.
         */
        assert(Objects.nonNull(msg.getKey()));
        HashRing.Hash hashedKey = new HashRing.Hash(msg.getKey());
        HashRing.HashRange acceptedRange = server.getMetdata()
            .getHashRing()
            .getServerHashRange(server.getMetdata().getName());
        if (!acceptedRange.inRange(hashedKey)) {
            logger.info("{}: Object key: <{}>->{} invalid." +
                "Accepted range=({},{}]",
                server.getMetdata().getName(),
                msg.getKey(), hashedKey.toHexString(),
                acceptedRange.getLower(), acceptedRange.getUpper()
            );
            replyMsg.setMetadata(server.getMetdata());
            replyMsg.setStatusType(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
            return replyMsg;
        }

        switch(msg.getStatusType()) {
            case PUT:
                replyMsg = handleClientPut(msg);
                break;
            case GET:
                replyMsg = handleClientGet(msg);
                break;
            default:
                msg.setStatusType(KVMessage.StatusType.CLIENT_ERROR);
                logger.info("Unrecognized message type: {}", msg.getStatusType());
        }

        return replyMsg;
    }

    private UnifiedMessage handleAdminMessage(UnifiedMessage msg) throws Exception {
        logger.info("HANDLE_ADMIN_MESSAGE: StatusType={}", msg.getStatus());
        KVServerMetadata metadata = msg.getMetadata();
        IECSNode.ECSNodeFlag flg = server.getStatus();
        UnifiedMessage.Builder respBuilder = new UnifiedMessage.Builder();

        try {
            switch (msg.getStatusType()) {
                case START:
                    server.update(metadata);
                    server.start();
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case STOP:
                    server.update(metadata);
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case SHUTDOWN:
                    server.update(metadata);
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    sendMessage(respBuilder.build());
                    server.shutdown();

                    /* Shouldn't return after server shuts down */
                    assert(false);                                     
                    break;
                case SERVER_INIT:
                    server.initKVServer(msg.getMetadata(),
                        msg.getCacheSize(), msg.getCacheStrategy());
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case SERVER_WRITE_LOCK:
                    server.lockWrite();
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case SERVER_WRITE_UNLOCK:
                    assert (flg.equals(IECSNode.ECSNodeFlag.KV_TRANSFER));
                    server.unLockWrite();
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case SERVER_MOVEDATA:
                    assert (flg.equals(IECSNode.ECSNodeFlag.KV_TRANSFER));
                    server.moveData(msg.getKeyRange(), msg.getServer());
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case SERVER_UPDATE:
                    server.update(msg.getMetadata());
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case SERVER_DUMP_DATA:
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withDataSet(server.getAllData())
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case SERVER_DUMP_REPLICA_DATA:
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withDataSet(server.getReplicaData(msg.getServer()))
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case ECS_HEARTBEAT:
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case RECOVER_DATA:
                    /*
                     * Functionality:
                     * Transfer data from THIS replica to msg.getServer()'s primary
                     * disk. (e.g. find a new home for this data)
                     * -> opposite of replication (recovery): replica sending data
                     *    to primary, rather than server sending data to replica.
                     * -> do not remove the data transferred from this replica.
                     *
                     * getServer(): server to send replicated data to (new primary)
                     * getKeyRange(): all objects within this keyrange should be
                     *                sent to getServer()
                     * getPrimary(): the old primary which the data in keyrange belonged to
                     */
                    logger.info("RECOVER_DATA for old primary: {}. {}:{} -> {} | range={}",
                        msg.getPrimary().getUuid(),
                        server.getHostname(), server.getPort(),
                        msg.getServer().getUuid(),
                        new HashRing.HashRange(msg.getKeyRange()));

                    server.replicaRecoverData(msg.getServer(),
                        msg.getPrimary(), msg.getKeyRange());
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case SERVER_REPLICATE:
                    server.update(msg.getMetadata());
                    server.updateReplicas();

                    HashRing _ring = msg.getMetadata().getHashRing();
                    List<String> replicas = _ring.getReplicas(
                        _ring.getServerByName(server.getMetdata().getName())
                    ).stream().map(ECSNode::getNodeName).collect(Collectors.toList());

                    logger.info("SERVER_REPLICATE: {}:{} -> {}",
                        server.getHostname(), server.getPort(), replicas);
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_ECS)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case SHOW_REPLICATION:
                    logger.info("SHOW_REPLICATION: {}:{}",
                        server.getHostname(), server.getPort());
                    server.requestReplicatedDisk();
		            break;
                default:
                    throw new Exception("Unrecognized message");
            }
        } catch (Exception e) {
            logger.error("Failed to handle ECS request", e);
            respBuilder
                .withMessageType(MessageType.SERVER_TO_ECS)
                .withMessage(e.getMessage())
                .withStatusType(KVMessage.StatusType.ERROR);
        }

        return respBuilder.build();
    }

    private UnifiedMessage handleServerMessage(UnifiedMessage msg) {
        logger.info("HANDLE_SERVER_MESSAGE: StatusType={}", msg.getStatus());
        UnifiedMessage.Builder respBuilder = new UnifiedMessage.Builder();

        try {
            switch (msg.getStatusType()) {
                case GET:
                    handleClientGet(msg);
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_SERVER)
                        .withStatusType(msg.getStatusType())
                        .withValue(msg.getValue());
                    break;
                case MAP:
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_SERVER)
                        .withStatusType(SUCCESS)
                        .withKey(handleMRMap(msg.getMrType(), msg.getKey()));
                    break;
                case REDUCE:
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_SERVER)
                        .withStatusType(SUCCESS)
                        .withKey(handleMRReduce(msg.getMrType(), msg.getKey()));
                    break;
                case PUT_MANY:
                    handleClientPut(msg);
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_SERVER)
                        .withStatusType(msg.getStatusType());
                    break;
                case PUT_DATA:
                    handleClientPut(msg);
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_SERVER)
                        .withStatusType(msg.getStatusType());
                    logger.info("PUT_DATA_SUCCESS: {}", msg.getKey());
                    break;
                case SERVER_MOVEDATA:
                    server.recvData(msg.getDataSet());
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_SERVER)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case PUT:
                    assert(Objects.nonNull(msg.getUUID()));
                    KVMessage.StatusType respType = server.replicate(
                        msg.getPrimary().getNodeName(), msg.getUUID(),
                        msg.getKey(), msg.getValue());
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_SERVER)
                        .withStatusType(respType);
                    break;
                case RECOVER_DATA:
                    logger.info("SERVER_RECOVER_DATA: {}:{} - REPLICA={}:{}",
                        server.getHostname(), server.getPort(),
                        msg.getServer());

                    server.recvData(msg.getDataSet());
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_SERVER)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case SHOW_REPLICATION:
                    server.printReplicatedDisk(msg.getPrimary());
                    respBuilder
                        .withMessageType(MessageType.SERVER_TO_SERVER)
                        .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case REPLICATE:
                    server.recvReplicatedData(msg.getDataSet(), msg.getPrimary());
                    respBuilder
                            .withMessageType(MessageType.SERVER_TO_SERVER)
                            .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                case UNDO_REPLICATE:
                    server.clearReplicatedData(msg.getPrimary());
                    respBuilder
                            .withMessageType(MessageType.SERVER_TO_SERVER)
                            .withStatusType(KVMessage.StatusType.SUCCESS);
                    break;
                default:
                    throw new Exception("Unrecognized message");
            }
        } catch (Exception e) {
            logger.error("Failed to handle ECS request", e);
            respBuilder
                .withMessageType(MessageType.SERVER_TO_SERVER)
                .withStatusType(KVMessage.StatusType.ERROR);
        }

        return respBuilder.build();
    }

    private UnifiedMessage handleClientPut(UnifiedMessage msg) {
        // If there is no UUID, that's a big no-no
        assert (Objects.nonNull(msg.getUUID()));

        KVDataSet dataSet = msg.getDataSet();
        if (Objects.isNull(dataSet)) {
            dataSet = new KVDataSet();
            dataSet.addEntry(new Pair<>(msg.getKey(), msg.getValue()));
        }
        logger.info("Received PUT DataSet='{}'", dataSet.toString());

        KVMessage.StatusType status = PUT_SUCCESS;
        List<Pair<String, String>> entries = dataSet.getEntries();
        for (Pair<String, String> pair : entries) {
            String key = pair.getKey(), value = pair.getValue();

            try {
                status = server.putKVWithStatusCheck(msg.getUUID(), key, value);
                logger.info("PUT: {} <{}, {}>", status, key, value);
            } catch (Exception e) {
                logger.error("PUT failed. <{}, {}>", key, value, e);

                // Delete scenario
                if (Objects.isNull(value) || value.equals("null") || value.equals("")) {
                    status = KVMessage.StatusType.DELETE_ERROR;
                }
                else {
                    status =  KVMessage.StatusType.PUT_ERROR;
                }
            }
        }

        logger.info("Success PUT DataSet='{}'", dataSet.toString());
        msg.setStatusType(status);
        return msg;
    }

    private UnifiedMessage handleClientGet(UnifiedMessage msg) {
        String key = msg.getKey();

        logger.info("Received GET <{}>", msg.getKey());
        KVMessage.StatusType status;
        try{
            String retrievedValue = this.server.getKV(key);
            msg.setValue(retrievedValue);

            status = KVMessage.StatusType.GET_SUCCESS;
            logger.info("GET: {} <{}, {}>", status, key, retrievedValue);
        } catch (Exception e) {
            status = KVMessage.StatusType.GET_ERROR;
            logger.error("GET <{}> failed. Status: {}", key, status, e);
        }

        msg.setStatusType(status);
        return msg;
    }

    private UnifiedMessage handleMRMaster(MapReduce.Type type, String[] keys) {
        UnifiedMessage.Builder responseBuilder = new UnifiedMessage.Builder()
            .withMessageType(MessageType.SERVER_TO_CLIENT);

        String[] results = null;
        boolean hasError = false;

        HashRing ring;
        ECSNode currentServer;


        ring = server.getMetdata().getHashRing();
        currentServer = ring.getServerByName(server.getMetdata().getName());
        try {
            results = MapReduceCtrl.masterMapReduce(type, currentServer, ring, keys);
        } catch (Exception e) {
            logger.error("Map failed, cannot continue..", e);
            hasError = true;
        }

        if (hasError || Objects.isNull(results)) {
            return responseBuilder
                .withStatusType(ERROR)
                .withMessage("Map failed, cannot continue..")
                .build();
        }

        // Successful
        return responseBuilder
            .withKeys(results)
            .build();
    }

    private String handleMRMap(MapReduce.Type type, String mapId) throws Exception {
        KVDataSet dataSet = new KVDataSet();
        BiConsumer<String, String> Emit = (key, value) -> {
            dataSet.addEntry(new Pair<>(key, value));
        };

        MapReduce mapper = null;
        switch (type) {
            case K_MEANS_CLUSTERING:
                DFS DFS = new DFS(server.getMetdata().getHashRing());
                mapper = new KMeansClustering(Emit, DFS);
                break;
            case WORD_FREQ:
                mapper = new WordFreq(Emit);
                break;
            case SORT:
                mapper = new Sort(Emit);
                break;
            default:
                throw new Exception(String.format(
                    "Unsupported MR Type: %s", type));
        }

        HashRing ring = server.getMetdata().getHashRing();
        String resultKey = null;
        String dataToMap;
        try {
            dataToMap = KVServerRequestLib.serverGetKV(ring, mapId).getValue();
            mapper.Map(new MapInput(dataToMap));

            if (dataSet.size() > 0) {
                resultKey = String.format("%s-%s",
                    "MAP-RESULT",
                    UUID.randomUUID().toString());

                Pair<String, String> mapResult = new Pair<>(
                    resultKey, new MapOutput(dataSet).toString()
                );
                KVServerRequestLib.serverPutKV(ring, mapResult);
            }
        } catch (Exception e) {
            logger.error("[MAPPER]: Error when Mapping data", e);
            throw e;
        }

        logger.info("[MAPPER]: Map Success: {}", mapId);
        return resultKey;
    }

    private String handleMRReduce(MapReduce.Type type, String partId) throws Exception {
        KVDataSet dataSet = new KVDataSet();
        BiConsumer<String, String> Emit = (key, value) -> {
            dataSet.addEntry(new Pair<>(key, value));
        };

        MapReduce mapper = null;
        switch (type) {
            case K_MEANS_CLUSTERING:
                DFS DFS = new DFS(server.getMetdata().getHashRing());
                mapper = new KMeansClustering(Emit, DFS);
                break;
            case WORD_FREQ:
                mapper = new WordFreq(Emit);
                break;
            case SORT:
                mapper = new Sort(Emit);
                break;
            default:
                throw new Exception(String.format(
                    "Unsupported MR Type: %s", type));
        }

        HashRing ring = server.getMetdata().getHashRing();
        String resultKey = null;
        ReduceInput.ReduceDTO dto;
        String dataToMap;
        try {
            dataToMap = KVServerRequestLib.serverGetKV(ring, partId).getValue();
            dto = new ReduceInput.ReduceDTO(dataToMap);

            for (ReduceInput input : dto.getInputs()) {
                mapper.Reduce(input);
            }

            if (dataSet.size() > 0) {
                resultKey = String.format("%s-%s",
                    "REDUCE-RESULT",
                    UUID.randomUUID().toString());

                Pair<String, String> mapResult = new Pair<>(
                    resultKey, new ReduceOutput(dataSet).toString()
                );
                KVServerRequestLib.serverPutKV(ring, mapResult);
            }
        } catch (Exception e) {
            logger.error("[MAPPER]: Error when Reducing data", e);
            throw e;
        }

        logger.info("[MAPPER]: Reduce Success: {}", partId);
        return resultKey;
    }

    /**
     * Method sends a Message using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(UnifiedMessage msg) throws Exception{
        // Convert KVMessage to JSON String
        String msgAsString = msg.serialize();
        logger.info("SEND MESSAGE: " + msgAsString);
        TCPSockModule.send(output, msgAsString);
    }

    public void gracefulClose() {
        System.out.printf("THREAD-%s: Gracefully close detected, closing...\n", getId());
        isOpen = false;
    }

    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Receive a string message from socket and deserialize into Message object
     * @return Message received through socket
     * @throws IOException
     */
    private KVMessage receiveMessage() throws IOException {
        KVMessage msg = null;
        String msgString = null;
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        msgString = reader.readLine();
        if (msgString != null) {
            try {
                System.out.println(msgString);
                msg = objectMapper.readValue(msgString, Message.class);
            }
            catch (IOException e){
                System.out.println("readValue casued IO expcetion");
            }
        }
        return msg;
    }

    private UnifiedMessage receiveMessage2() throws IOException {
        UnifiedMessage msg = null;
        String msgString = null;
        msgString = TCPSockModule.recv(input);
        if (Objects.isNull(msgString)) {
            throw new IOException("Connection lost");
        }

        if (!msgString.equals("")) {
            try {
                //logger.info("Received message: {}", msgString);
                msg = new UnifiedMessage().deserialize(msgString);
                logger.info("Deserialized message: messageType={}, statusType={}",
                    msg.getMessageType(), msg.getStatusType());
            }
            catch (Exception e) {
                logger.error("Could not read value from receive", e);
            }
        }

        return msg;
    }
}
