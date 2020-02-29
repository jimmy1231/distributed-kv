package app_kvECS.impl;

import app_kvECS.HashRing;
import app_kvECS.KVServerMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

public class HashRingImpl extends HashRing {
    public HashRingImpl() {
        super(new TreeMap<>(), new HashMap<>());
    }

    /**
     * Works like a filter function, except on the list of servers
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
    public List<KVServerMetadata> filterServer(Predicate<KVServerMetadata> pred) {
        return null;
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
    public KVServerMetadata getServerByHash(Hash hash) {
        return null;
    }

    @Override
    public KVServerMetadata getServerByName(String serverName) {
        return null;
    }

    @Override
    public KVServerMetadata getServerByObjectKey(String objectKey) {
        return null;
    }

    /**
     * {@link #removeServerByHash(Hash)}
     * Remove the server at the exact position of the hash.
     * Note that this is contrary to {@link #getServerByHash(Hash)},
     * where the hash matches with the nearest server.
     * {@link #removeServerByName(String)}
     * Removes the server by its server name
     * {@link #addServer(KVServerMetadata)}
     * Computes the hash for the server, then adds the server
     * to the HashRing according to ascending hash order.
     *
     * @param hash
     */
    @Override
    public void removeServerByHash(Hash hash) {

    }

    @Override
    public void removeServerByName(String serverName) {

    }

    @Override
    public void addServer(KVServerMetadata server) {

    }

    /**
     * {@link #getSuccessorServer(KVServerMetadata)}
     * Gets the server immediately succeeding the current
     * server in the HashRing (look "ahead of" the current server)
     * {@link #getPredecessorServer(KVServerMetadata)}
     * Gets the server immediately preceding the current
     * server in the HashRing (look "behind" the current server)
     *
     * @param server
     */
    @Override
    public KVServerMetadata getSuccessorServer(KVServerMetadata server) {
        return null;
    }

    @Override
    public KVServerMetadata getPredecessorServer(KVServerMetadata server) {
        return null;
    }

    /**
     * {@link #getServerHashRange(KVServerMetadata)}
     * Gets the HashRange for the specified server given
     * the server object
     * {@link #getServerHashRange(String)}
     * Gets the HashRange for the specified server given
     * the name of the server
     *
     * @param server
     */
    @Override
    public HashRange getServerHashRange(KVServerMetadata server) {
        return null;
    }

    @Override
    public HashRange getServerHashRange(String serverName) {
        return null;
    }
}
