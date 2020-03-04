package app_kvECS.impl;

import app_kvECS.HashRing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.reflect.TypeToken;
import ecs.ECSNode;
import ecs.IECSNode;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class HashRingImpl extends HashRing {
    /**
     * For serialization/deserialization purposes
     */
    private Type ringType = new TypeToken<TreeMap<Hash, ECSNode>>() {}.getType();
    private Type serversType = new TypeToken<Map<String, ECSNode>>() {}.getType();
    private Gson HASH_RING_GSON = new GsonBuilder()
        .enableComplexMapKeySerialization()
        .excludeFieldsWithoutExposeAnnotation()
        .create();

    private class SerializedHashRing {
        @Expose
        String serializedRing;
        @Expose
        String serializedServers;
    }

    public HashRingImpl() {
        super(
            new TreeMap<Hash, ECSNode>(),
            new HashMap<String, ECSNode>()
        );
    }

    /**
     * Works like a findAndRemove function, except on the list of servers
     * in the HashRing.
     * <p>
     * Example: The following predicate function would return all
     * servers in the HashRing with serverStatusType == 'STOPPED'.
     *
     * <pre>
     * HashRing.filterServer(
     *      (KVServerMetadata server) -> {
     *          return server.serverStatusType == 'STOPPED'
     *      }
     * );
     * </pre>
     *
     * @param pred A predicate. If return true, then the evaluating
     *             object is included in the output list.
     * @return A list of filtered KVServerMetadata.
     */
    @Override
    public List<ECSNode> filterServer(Predicate<ECSNode> pred) {
        List<ECSNode> result = new ArrayList<>();

        ECSNode server;
        for (Map.Entry<String, ECSNode> entry : servers.entrySet()) {
            server = entry.getValue();
            if (pred.test(server)) {
                result.add(server);
            }
        }

        return result;
    }

    @Override
    public void forEachServer(Consumer<ECSNode> consumer) {
        for (Map.Entry<String, ECSNode> entry : servers.entrySet()) {
            consumer.accept(entry.getValue());
        }
    }

    /**
     * {@link #getServerByHash(Hash)}
     * Gets nearest server -> traverse HashRing in CW order.
     * {@link #getServerByName(String)}
     * Gets server by its server name
     * {@link #getServerByObjectKey(String)}
     * Hashes objectKey using MD5, then takes the computed hash
     * and gets nearest server traversing CW order around HashRing
     *
     * @param hash
     */
    @Override
    public ECSNode getServerByHash(Hash hash) {
        ECSNode ecsNode = ring.get(hash);

        Map.Entry<Hash, ECSNode> entry;
        if (Objects.isNull(ecsNode)) {
            entry = ring.higherEntry(hash);

            /*
             * Check wrap-around (ring) logic. If the hash presented is
             * greater than any entry in the ring, then wrap-around
             * to the first entry. Else, return the immediately greater
             * ECSNode following the hash position.
             */
            if (Objects.nonNull(entry)) {
                ecsNode = entry.getValue();
            } else if (ring.size() > 0) {
                entry = ring.firstEntry();
                assert(Objects.nonNull(entry));
                ecsNode = entry.getValue();
            } else {
                ecsNode = null;
            }
        }

        return ecsNode;
    }

    @Override
    public ECSNode getServerByName(String serverName) {
        return servers.get(serverName);
    }

    @Override
    public ECSNode getServerByObjectKey(String objectKey) {
        Hash hash = new Hash(objectKey);
        return getServerByHash(hash);
    }

    /**
     * {@link #removeServerByHash(Hash)}
     * Remove the server at the exact position of the hash.
     * Note that this is contrary to {@link #getServerByHash(Hash)},
     * where the hash matches with the nearest server.
     * {@link #removeServerByName(String)}
     * Removes the server by its server name
     * {@link #addServer(ECSNode)}
     * Computes the hash for the server, then adds the server
     * to the HashRing according to ascending hash order.
     *
     * @param hash
     */
    @Override
    public void removeServerByHash(Hash hash) {
        ECSNode server = ring.get(hash);
        if (Objects.isNull(server)) {
            return;
        }

        removeServer(server);
    }

    @Override
    public void removeServerByName(String serverName) {
        ECSNode server = servers.get(serverName);
        if (Objects.isNull(server)) {
            return;
        }

        removeServer(server);
    }

    @Override
    public void removeServer(ECSNode server) {
        server.setEcsNodeFlag(IECSNode.ECSNodeFlag.START_STOP);
    }

    @Override
    public void addServer(ECSNode server) {
        System.out.println("ADD SERVER: " + new Gson().toJson(server));
        ECSNode _server = servers.get(server.getNodeName());
        if (Objects.nonNull(_server)) {
            return;
        }

        servers.put(server.getNodeName(), server);
    }

    private void recomputeHashRanges() {
        ECSNode server;
        Iterator<Map.Entry<Hash, ECSNode>> it = ring.entrySet().iterator();
        while (it.hasNext()) {
            server = it.next().getValue();

            HashRange range = getServerHashRange(server);
            assert(Objects.nonNull(range));
            server.setNodeHashRange(range.toArray());
        }
    }

    @Override
    public void updateRing() {
        /*
         * Step 1:
         * (a) Add all ECSNode's that are in IDLE_START state
         *     to hashRing
         * (b) Set ECSNode state to START
         * (c) After all is done, recompute the hashRange of
         *     every server in the HashRing
         */
        {
            ECSNode server;
            Iterator<Map.Entry<String, ECSNode>> it = servers.entrySet().iterator();
            IECSNode.ECSNodeFlag flag;
            Hash hash;
            while (it.hasNext()) {
                server = it.next().getValue();
                flag = server.getEcsNodeFlag();
                if (flag.equals(IECSNode.ECSNodeFlag.IDLE_START)) {
                    /* IMPORTANT: put to ring */
                    hash = new Hash(server.getUuid());
                    ring.put(hash, server);
                }
            }
            recomputeHashRanges();
        }

        /*
         * Step 2:
         * (a) Remove all ECSNode's that are in START_STOP state
         *     from hashRing (do not touch the servers)
         * (b) Set ECSNode state to STOP
         * (c) After all is done, recompute the hash ranges
         *     of all servers in the HashRing.
         */
        {
            ECSNode server;
            Iterator<Map.Entry<String, ECSNode>> it = servers.entrySet().iterator();
            IECSNode.ECSNodeFlag flag;
            while (it.hasNext()) {
                server = it.next().getValue();
                flag = server.getEcsNodeFlag();
                if (flag.equals(IECSNode.ECSNodeFlag.START_STOP)) {
                    /* IMPORTANT: remove from ring */
                    ECSNode removed = ring.remove(new Hash(server.getUuid()));
                    server.setEcsNodeFlag(IECSNode.ECSNodeFlag.STOP);
                    server.setNodeHashRange(null);

                    assert(removed.getUuid().equals(server.getUuid()));
                }
            }
            recomputeHashRanges();
        }
    }

    /**
     * {@link #getSuccessorServer(ECSNode)}
     * Gets the server immediately succeeding the current
     * server in the HashRing (look "ahead of" the current server)
     * {@link #getPredecessorServer(ECSNode)}
     * Gets the server immediately preceding the current
     * server in the HashRing (look "behind" the current server)
     *
     * @param server
     */
    @Override
    public ECSNode getSuccessorServer(ECSNode server) {
        Hash hash = new Hash(server.getUuid());
        assert(ring.get(hash).getNodeName().equals(server.getNodeName()));

        /*
         * There are 3 cases:
         * (1) Current server is the largest server in the ring
         *      -> Wrap-around
         * (2) Current server is the only server in the ring
         *      -> No successor
         * (3) Current server is in the "middle" (or smallest) of the ring
         *      -> Return successor
         */
        ECSNode successor;
        Map.Entry<Hash, ECSNode> entry = ring.higherEntry(hash);
        if (Objects.isNull(entry)) {
            // (1)
            if (ring.size() > 1) {
                entry = ring.firstEntry();
                successor = entry.getValue();
                assert(!successor.getNodeName().equals(server.getNodeName()));
            }
            // (2)
            else {
                assert(ring.size() == 1);
                successor = null;
            }
        }
        // (3)
        else {
            successor = entry.getValue();
        }

        return successor;
    }

    @Override
    public ECSNode getPredecessorServer(ECSNode server) {
        Hash hash = new Hash(server.getUuid());
        assert(ring.get(hash).getNodeName().equals(server.getNodeName()));

        /*
         * There are 3 cases:
         * (1) Current server is the smallest server in the ring
         *      -> Wrap-around
         * (2) Current server is the only server in the ring
         *      -> No predecessor
         * (3) Current server is in the "middle" (or largest) in the ring
         *      -> Return predecessor
         */
        ECSNode predecessor;
        Map.Entry<Hash, ECSNode> entry = ring.lowerEntry(hash);
        if (Objects.isNull(entry)) {
            // (1)
            if (ring.size() > 1) {
                entry = ring.lastEntry();
                predecessor = entry.getValue();
                assert(!predecessor.getNodeName().equals(server.getNodeName()));
            }
            // (2)
            else {
                assert(ring.size() == 1);
                predecessor = null;
            }
        }
        // (3)
        else {
            predecessor = entry.getValue();
        }

        return predecessor;
    }

    /**
     * {@link #getServerHashRange(ECSNode)}
     * Gets the HashRange for the specified server given
     * the server object
     * {@link #getServerHashRange(String)}
     * Gets the HashRange for the specified server given
     * the name of the server
     *
     * @param server
     */
    @Override
    public HashRange getServerHashRange(ECSNode server) {
        ECSNode predecessor = getPredecessorServer(server);
        if (Objects.isNull(predecessor)) {
            predecessor = server;
        }

        Hash lower = new Hash(predecessor.getUuid());
        Hash upper = new Hash(server.getUuid());
        return new HashRange(lower, upper);
    }

    @Override
    public HashRange getServerHashRange(String serverName) {
        ECSNode server = getServerByName(serverName);
        if (Objects.isNull(server)) {
            return null;
        }

       return getServerHashRange(server);
    }

    /**
     * {@link #serialize()}
     * Serializes this class so that it can be passed through the
     * network.
     * {@link #deserialize(String)}
     * Deserializes this class. First create an empty instance, then
     * call this function with the JSON string. This will populate
     * HashRing by deserializing the data in the JSON.
     */
    @Override
    public String serialize() {
        SerializedHashRing serialized = new SerializedHashRing();

        serialized.serializedRing = HASH_RING_GSON.toJson(this.getRing());
        serialized.serializedServers = HASH_RING_GSON.toJson(this.getServers());

        return HASH_RING_GSON.toJson(serialized);
    }

    @Override
    public HashRing deserialize(String json) {
        SerializedHashRing serialized = HASH_RING_GSON.fromJson(
            json, SerializedHashRing.class
        );

        this.ring = HASH_RING_GSON.fromJson(serialized.serializedRing, ringType);
        this.servers = HASH_RING_GSON.fromJson(serialized.serializedServers, serversType);

        return this;
    }

    @Override
    public TreeMap<Hash, ECSNode> getRing() {
        return this.ring;
    }

    @Override
    public Map<String, ECSNode> getServers() {
        return this.servers;
    }

    @Override
    public void setRing(TreeMap<Hash, ECSNode> ring) {
        this.ring = ring;
    }

    @Override
    public void setServers(Map<String, ECSNode> servers) {
        this.servers = servers;
    }

    @Override
    public void print() {
        StringBuilder sb = new StringBuilder();
        sb.append("=============HASH-RING================\n");
        {
            Iterator<Map.Entry<Hash, ECSNode>> it = ring.entrySet().iterator();
            Map.Entry<Hash, ECSNode> entry;
            ECSNode ecsNode;
            String[] hashRange;
            Hash hash;
            int i = 0;
            while (it.hasNext()) {
                entry = it.next();
                hash = entry.getKey();
                ecsNode = entry.getValue();
                hashRange = ecsNode.getNodeHashRange();

                sb.append(String.format("%3d: %s => %s | RANGE=(%s,%s] | %s\n",
                    i, ecsNode.getNodeName(), hash.toHexString(),
                    hashRange[0], hashRange[1],
                    ecsNode.getEcsNodeFlag())
                );

                i++;
            }
        }

        sb.append("=============ALL-SERVERS============\n");
        {
            Iterator<Map.Entry<String, ECSNode>> it = servers.entrySet().iterator();
            Map.Entry<String, ECSNode> entry;
            ECSNode ecsNode;
            while (it.hasNext()) {
                entry = it.next();
                ecsNode = entry.getValue();

                sb.append(String.format("%s: %s:%d | %s\n",
                    ecsNode.getNodeName(), ecsNode.getNodeHost(),
                    ecsNode.getNodePort(), ecsNode.getEcsNodeFlag())
                );
            }
        }
        System.out.print(sb.toString());
    }
}
