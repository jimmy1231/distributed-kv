package app_kvServer;

import app_kvECS.HashRing;
import app_kvECS.TCPSockModule;
import app_kvServer.dsmr.MapReduce;
import app_kvECS.ECSNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.Pair;
import shared.messages.KVDataSet;
import shared.messages.KVMessage;
import shared.messages.MessageType;
import shared.messages.UnifiedMessage;

import java.util.*;

public class ServerRequestLib {
    private static final Logger logger = LoggerFactory.getLogger(ServerRequestLib.class);

    public static void replicaRecoverData(ECSNode replicaServer,
                                          ECSNode dest,
                                          KVDataSet dataSet) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.SERVER_TO_SERVER)
            .withServer(replicaServer)
            .withStatusType(KVMessage.StatusType.RECOVER_DATA)
            .withDataSet(dataSet)
            .build();

        send(dest, msg);
    }

    public static Pair<String, String> serverGetKV(HashRing ring,
                                                   String key) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.SERVER_TO_SERVER)
            .withStatusType(KVMessage.StatusType.GET)
            .withKey(key)
            .build();

        ECSNode server = ring.getServerByObjectKey(key);
        UnifiedMessage resp = send(server, msg);
        logger.info("Success Server getKV " +
                "-> {}, Key=<{}>, Data='{}'",
            server.getUuid(), key, resp.getValue());

        return new Pair<>(key, resp.getValue());
    }

    public static void serverPutKV(HashRing ring,
                                   Pair<String, String> entry) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.SERVER_TO_SERVER)
            .withStatusType(KVMessage.StatusType.PUT_DATA)
            .withKey(entry.getKey())
            .withValue(entry.getValue())
            .withUUID(UUID.randomUUID())
            .build();

        ECSNode server = ring.getServerByObjectKey(entry.getKey());
        send(server, msg);
        logger.info("Success Server putKV " +
                "-> {}, Key=<{}>, Data='{}'",
            server.getUuid(),
            entry.getKey(), entry.getValue());
    }

    public static String serverDoMap(ECSNode mapper,
                                     String mapId,
                                     MapReduce.Type mrType) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.SERVER_TO_SERVER)
            .withStatusType(KVMessage.StatusType.MAP)
            .withMrType(mrType)
            .withKey(mapId)
            .build();

        String mapResult = send(mapper, msg).getKey();
        logger.info("Success Map! Mapper='{}', MapId='{}', MapResult={}",
            mapper.getUuid(), mapId, mapResult);
        return mapResult;
    }

    public static String serverDoReduce(ECSNode reducer,
                                        String reduceId,
                                        MapReduce.Type mrType) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.SERVER_TO_SERVER)
            .withStatusType(KVMessage.StatusType.REDUCE)
            .withMrType(mrType)
            .withKey(reduceId)
            .build();

        String mapResult = send(reducer, msg).getKey();
        logger.info("Success Reduce! Reducer='{}', ReduceId='{}', ReduceResult={}",
            reducer.getUuid(), reduceId, mapResult);
        return mapResult;
    }

    public static void serverPutMany(ECSNode server,
                                     KVDataSet dataSet) throws Exception {
        UnifiedMessage msg = new UnifiedMessage.Builder()
            .withMessageType(MessageType.SERVER_TO_SERVER)
            .withStatusType(KVMessage.StatusType.PUT_MANY)
            .withDataSet(dataSet)
            .build();

        send(server, msg);
        logger.info("Success Put Many: Server='{}', DataSet='{}'",
            server, dataSet.toString());
    }

    public static void serverDeleteAll(HashRing ring,
                                       List<String> keys) throws Exception {
        Map<String, List<String>> serversAndKeys = new HashMap<>();

        List<String> keys4Server;
        ECSNode server;
        String serverName;
        for (String key : keys) {
            server = ring.getServerByObjectKey(key);
            serverName = server.getNodeName();

            keys4Server = serversAndKeys.get(serverName);
            if (Objects.isNull(keys4Server)) {
                serversAndKeys.put(serverName, new ArrayList<>());
                keys4Server = serversAndKeys.get(serverName);
            }

            keys4Server.add(key);
        }
        logger.info("SERVER_DELETE_ALL: {}", serversAndKeys);

        Iterator<Map.Entry<String, List<String>>> it;
        it = serversAndKeys.entrySet().iterator();

        Map.Entry<String, List<String>> entry;
        KVDataSet dataSet;
        while (it.hasNext()) {
            entry = it.next();

            dataSet = new KVDataSet();
            for (String _key : entry.getValue()) {
                logger.info("ADD_TO_DATASET: server={}, mapid={}",
                    entry.getKey(), _key);
                dataSet.addEntry(new Pair<>(_key, ""));
            }

            try {
                serverPutMany(
                    ring.getServerByName(entry.getKey()),
                    dataSet
                );
            } catch (Exception e) {
                logger.error("Failed to delete for Server={}. DataSet={}",
                    entry.getKey(), dataSet.toString());
                /* Swallow */
            }
        }

    }

    private static UnifiedMessage send(ECSNode server, UnifiedMessage msg) throws Exception {
        return send(server.getNodeHost(), server.getNodePort(), msg);
    }

    private static UnifiedMessage send(String host, int port, UnifiedMessage msg) throws Exception {
        TCPSockModule module = null;
        UnifiedMessage resp = null;
        try {
            module = new TCPSockModule(host, port);
            resp = module.doRequest(msg);
        } catch (Exception e) {
            throw e;
        } finally {
            if (Objects.nonNull(module)) {
                module.close();
            }
        }

        return resp;
    }
}
