package app_kvECS;

import ecs.ECSNode;
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public abstract class HashRing {
    private static Logger logger = Logger.getLogger(HashRing.class);
    private static MessageDigest md;
    static {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            logger.error("No such algorithm: MD5");
        }
    }

    public static byte[] md5(String stringToHash) {
        return md.digest(stringToHash.getBytes());
    }

    /**
     * Key: MD5 hash of server
     * Value: Server name (e.g. server1)
     */
    protected Map<Hash, String> ring;

    /**
     * Separate data structure to store the server info
     * Key; Server name (e.g. server1)
     * Value: Server KVServerMetadata - port, IP, etc.
     * @see ECSNode
     */
    protected Map<String, ECSNode> servers;

    /**
     * Hash ring key. Encapsulates the byte array logic that
     * is necessary to store the MD5 hashes.
     * The object implements Comparable, which allows it to
     * be directly used as a key object for java.util.Map<K, V>
     */
    public class Hash implements Comparable<Hash> {
        byte[] hashBytes;

        public Hash(String stringToHash) {
            hashBytes = md5(stringToHash);
        }

        @Override
        public int compareTo(Hash o) {
            /*
             * Most significant byte is at index 0. So we
             * do byte-wise comparison, and return immediately
             * if a byte is bigger/smaller.
             * If bytes are equal, iterate to the next byte.
             * If all bytes have been traversed, then 2 hashes
             * are equal.
             */
            assert(hashBytes.length == o.hashBytes.length);
            int numBytes = hashBytes.length;

            byte b1, b2;
            int i;
            for (i=0; i<numBytes; i++) {
                b1 = hashBytes[i];
                b2 = o.hashBytes[i];

                if (b1 < b2) {
                    return -1;
                } else if (b1 > b2) {
                    return 1;
                }
            }

            return 0;
        }

        @Override
        public String toString() {
            return DatatypeConverter.printHexBinary(hashBytes);
        }
    }

    /**
     * Encapsulating class for the hash range.
     *
     * Lower inclusive
     * Upper exclusive
     */
    public class HashRange {
        private Long lower;
        private Long upper;

        public HashRange(Long lower, Long upper) {
            this.lower = lower;
            this.upper = upper;
        }

        public boolean inRange(Long value) {
            return value >= lower && value < upper;
        }
    }

    /****************************************************/
    protected HashRing(Map<Hash, String> ring, Map<String, ECSNode> servers) {
        super();
        this.ring = ring;
        this.servers = servers;
    }
    /****************************************************/

    //////////////////////////////////////////////////////////////
    /**
     * Works like a filter function, except on the list of servers
     * in the HashRing.
     *
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
    public abstract List<ECSNode> filterServer(Predicate<ECSNode> pred);

    /**
     * {@link #getServerByHash(Hash)}
     *      Gets nearest server -> traverse HashRing in CW order.
     * {@link #getServerByName(String)}
     *      Gets server by its server name
     * {@link #getServerByObjectKey(String)}
     *      Hashes objectKey using MD5, then takes the computed hash
     *      and gets nearest server traversing CW order around HashRing
     */
    public abstract ECSNode getServerByHash(Hash hash);
    public abstract ECSNode getServerByName(String serverName);
    public abstract ECSNode getServerByObjectKey(String objectKey);

    /**
     * {@link #removeServerByHash(Hash)}
     *      Remove the server at the exact position of the hash.
     *      Note that this is contrary to {@link #getServerByHash(Hash)},
     *      where the hash matches with the nearest server.
     * {@link #removeServerByName(String)}
     *      Removes the server by its server name
     * {@link #addServer(ECSNode)}
     *      Computes the hash for the server, then adds the server to a Map
     *      structure.
     *      IMPORTANT: Calling this function does NOT add the server to the
     *      hashRing itself. It simply "records" that the server is now
     *      present in the system. To "persist" the server, {@link #updateRing()}
     *      is called.
     * {@link #updateRing()}
     *      Persist any changes (e.g. addServer, removeServer) to the
     *      hashRing since the last time this function was called.
     *
     *      Example 1:
     *      <pre>
     *          // Initial: hashRing={}
     *          hashRing.addServer(A); // hashRing={}
     *          hashRing.addServer(B); // hashRing={}
     *          hashRing.updateRing(); // hashRing={A, B}
     *      </pre>
     *
     *      Example 2:
     *      <pre>
     *          // Initial: hashRing={A, B, C, D}
     *          hashRing.removeServer(A); // hashRing={A, B, C, D}
     *          hashRing.addServer(E); // hashRing={A, B, C, D}
     *          hashRing.updateRing(); // hashRing={B, C, D, E}
     *      </pre>
     */
    public abstract void removeServerByHash(Hash hash);
    public abstract void removeServerByName(String serverName);
    public abstract void addServer(ECSNode server);
    public abstract void updateRing();

    /**
     * {@link #getSuccessorServer(ECSNode)}
     *      Gets the server immediately succeeding the current
     *      server in the HashRing (look "ahead of" the current server)
     * {@link #getPredecessorServer(ECSNode)}
     *      Gets the server immediately preceding the current
     *      server in the HashRing (look "behind" the current server)
     */
    public abstract ECSNode getSuccessorServer(ECSNode server);
    public abstract ECSNode getPredecessorServer(ECSNode server);

    /**
     * {@link #getServerHashRange(ECSNode)}
     *      Gets the HashRange for the specified server given
     *      the server object
     * {@link #getServerHashRange(String)}
     *      Gets the HashRange for the specified server given
     *      the name of the server
     */
    public abstract HashRange getServerHashRange(ECSNode server);
    public abstract HashRange getServerHashRange(String serverName);
    //////////////////////////////////////////////////////////////
}
