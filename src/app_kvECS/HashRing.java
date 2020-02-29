package app_kvECS;

import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
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

    /**
     * Key: MD5 hash of server
     * Value: Server name (e.g. server1)
     */
    protected Map<Hash, String> ring;

    /**
     * Separate data structure to store the server info
     * Key; Server name (e.g. server1)
     * Value: Server KVServerMetadata - port, IP, etc.
     * @see KVServerMetadata
     */
    protected Map<String, KVServerMetadata> servers;

    /**
     * Hash ring key. Encapsulates the byte array logic that
     * is necessary to store the MD5 hashes.
     * The object implements Comparable, which allows it to
     * be directly used as a key object for java.util.Map<K, V>
     */
    public class Hash implements Comparable<Hash> {
        byte[] hashBytes;

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
    protected HashRing(Map<Hash, String> ring, Map<String, KVServerMetadata> servers) {
        super();
        this.ring = ring;
        this.servers = servers;
    }

    public static String md5(String stringToHash) {
        byte[] bytes = stringToHash.getBytes();
        byte[] hash = md.digest(bytes);

        return Arrays.toString(hash);
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
    public abstract List<KVServerMetadata> filterServer(Predicate<KVServerMetadata> pred);

    /**
     * {@link #getServerByHash(Hash)}
     *      Gets nearest server -> traverse HashRing in CW order.
     * {@link #getServerByName(String)}
     *      Gets server by its server name
     * {@link #getServerByObjectKey(String)}
     *      Hashes objectKey using MD5, then takes the computed hash
     *      and gets nearest server traversing CW order around HashRing
     */
    public abstract KVServerMetadata getServerByHash(Hash hash);
    public abstract KVServerMetadata getServerByName(String serverName);
    public abstract KVServerMetadata getServerByObjectKey(String objectKey);

    /**
     * {@link #removeServerByHash(Hash)}
     *      Remove the server at the exact position of the hash.
     *      Note that this is contrary to {@link #getServerByHash(Hash)},
     *      where the hash matches with the nearest server.
     * {@link #removeServerByName(String)}
     *      Removes the server by its server name
     * {@link #addServer(KVServerMetadata)}
     *      Computes the hash for the server, then adds the server
     *      to the HashRing according to ascending hash order.
     */
    public abstract void removeServerByHash(Hash hash);
    public abstract void removeServerByName(String serverName);
    public abstract void addServer(KVServerMetadata server);

    /**
     * {@link #getSuccessorServer(KVServerMetadata)}
     *      Gets the server immediately succeeding the current
     *      server in the HashRing (look "ahead of" the current server)
     * {@link #getPredecessorServer(KVServerMetadata)}
     *      Gets the server immediately preceding the current
     *      server in the HashRing (look "behind" the current server)
     */
    public abstract KVServerMetadata getSuccessorServer(KVServerMetadata server);
    public abstract KVServerMetadata getPredecessorServer(KVServerMetadata server);

    /**
     * {@link #getServerHashRange(KVServerMetadata)}
     *      Gets the HashRange for the specified server given
     *      the server object
     * {@link #getServerHashRange(String)}
     *      Gets the HashRange for the specified server given
     *      the name of the server
     */
    public abstract HashRange getServerHashRange(KVServerMetadata server);
    public abstract HashRange getServerHashRange(String serverName);
    //////////////////////////////////////////////////////////////
}
