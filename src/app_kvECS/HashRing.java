package app_kvECS;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.gson.annotations.Expose;
import ecs.ECSNode;
import ecs.IECSNode;
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

public abstract class HashRing {
    private static Logger logger = Logger.getLogger(HashRing.class);
    private static MessageDigest md;
    protected int numOfServersInRing;

    private static byte[] MAX_MD5_HASH_BYTES = {
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
    };
    private static byte[] MIN_MD5_HASH_BYTES = {
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00
    };
    private static Hash MIN_MD5_HASH = new Hash(MIN_MD5_HASH_BYTES);
    private static Hash MAX_MD5_HASH = new Hash(MAX_MD5_HASH_BYTES);

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

    public int getNumOfServers(){ return numOfServersInRing; }

    /**
     * Key: MD5 hash of server
     * Value: Server name (e.g. server1)
     */
    @Expose
    protected TreeMap<Hash, ECSNode> ring;

    /**
     * Separate data structure to store the server info
     * Key; Server name (e.g. server1)
     * Value: Server KVServerMetadata - port, IP, etc.
     * @see ECSNode
     */
    @Expose
    protected Map<String, ECSNode> servers;

    /**
     * Hash ring key. Encapsulates the byte array logic that
     * is necessary to store the MD5 hashes.
     * The object implements Comparable, which allows it to
     * be directly used as a key object for java.util.Map<K, V>
     */
    public static class Hash implements Comparable<Hash> {
        @Expose
        byte[] hashBytes;

        public Hash(String stringToHash) {
            hashBytes = md5(stringToHash);
        }

        public Hash(byte[] bytes) {
            hashBytes = bytes;
        }

        public Hash(String hexString, boolean isHexString) {
            assert(isHexString);
            hashBytes = DatatypeConverter.parseHexBinary(hexString);
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
             *
             * Notice that we AND byte with 0xFF. This is because
             * Java stores bytes as signed data. So byte = 0xFF (255)
             * would actually be -1. This causes issues where a
             * byte, say 0xF3 is actually greater than 0x48, but
             * since 0xF3 is interpretted by Java as -13, 0x48 is
             * 72, 0x48 will actually evaluate to be greater than
             * 0xF3! To prevent this, we cast the bytes to a larger
             * data type -> int32, then bitmask away everything except
             * the least-significant byte by bitwise AND 0xFF.
             */
            assert(hashBytes.length == o.hashBytes.length);
            int numBytes = hashBytes.length;

            int b1, b2;
            int i;
            for (i=0; i<numBytes; i++) {
                b1 = ((int) hashBytes[i]) & 0xFF;
                b2 = ((int)o.hashBytes[i]) & 0xFF;

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
            return Arrays.toString(hashBytes);
        }

        public String toHexString() {
            return DatatypeConverter.printHexBinary(hashBytes);
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Hash) {
                return this.compareTo((Hash) o) == 0;
            } else {
                return false;
            }
        }

        public boolean gt(Hash h) {
            return compareTo(h) > 0;
        }

        public boolean lt(Hash h) {
            return compareTo(h) < 0;
        }

        public boolean gte(Hash h) {
            return compareTo(h) >= 0;
        }

        public boolean lte(Hash h) {
            return compareTo(h) <= 0;
        }
    }

    /**
     * Encapsulating class for the hash range:
     * (lower, upper]
     *
     * Upper is the hash of the server, lower is the hash of the
     * predecessor server + 1 (as in +1 of the least significant
     * bit in the MD5 hash)
     *
     * Lower: exclusive
     * Upper: inclusive
     */
    public static class HashRange {
        private Hash lower;
        private Hash upper;

        public HashRange(Hash lower, Hash upper) {
            this.lower = lower;
            this.upper = upper;
        }

        public HashRange(String[] hexRange) {
            assert(hexRange.length == 2);
            this.lower = new Hash(hexRange[0], true);
            this.upper = new Hash(hexRange[1], true);
        }

        public String[] toArray() {
            return new String[]{
                lower.toHexString(), upper.toHexString()
            };
        }

        public Hash getLower() {
            return lower;
        }

        public Hash getUpper() {
            return upper;
        }

        public boolean inRange(Hash value) {
            if (upper.gt(lower)) {
                return value.gt(lower) && value.lte(upper);
            }

            /*
             * Wrapped-around case. How to handle:
             * There are 2 sub-cases of this case. Consider the
             * following "flattened" hashRing:
             *
             *      ...  98 99 0 1 2  ...
             * ----------------|----------------------
             *        ^==================^
             *     lower               upper
             *
             * As can be seen, lower and upper are "wrapped"
             * around the hashRing.
             *
             * (1) Value is between (lower, MAX_RING_HASH]
             * (2) Value is between [MIN_RING_HASH, upper]
             *
             * If either of those cases evaluate to true, then
             * return TRUE. Else, return false.
             */
            if (lower.gt(upper)) {
                return (value.gt(lower) && value.lte(MAX_MD5_HASH))
                    || (value.gte(MIN_MD5_HASH) && value.lte(upper));
            }

            if (upper.equals(lower)) {
                return true;
            }

            assert(false); // should never get here
            return false;
        }
    }

    /****************************************************/
    public HashRing() {
        /* Default constructor */
    }

    public HashRing(TreeMap<Hash, ECSNode> ring, Map<String, ECSNode> servers) {
        super();
        this.ring = ring;
        this.servers = servers;
        numOfServersInRing = 0;
    }
    /****************************************************/

    //////////////////////////////////////////////////////////////
    /**
     * {@link #filterServer(Predicate)}
     *      Returns a list of servers that all evaluated to TRUE for
     *      the predicate. Returns empty list if no servers evaluated
     *      to TRUE.
     *
     *      Example usage:
     *      <pre>
     *      HashRing.filterServer((ECSNode server) -> {
     *          return server.serverStatusType == 'STOPPED'
     *      });
     *      </pre>
     * {@link #forEachServer(Consumer)}
     *      Iterates through the servers one by one (in no particular
     *      order).
     * {@link #findServer(Predicate)}
     *      Returns the first server that evaluates to TRUE for the
     *      predicate. If no servers evaluate to TRUE, then returns
     *      NULL.
     */
    public abstract List<ECSNode> filterServer(Predicate<ECSNode> pred);
    public abstract void forEachServer(Consumer<ECSNode> consumer);
    public abstract ECSNode findServer(Predicate<ECSNode> pred);

    /**
     * {@link #getServerByHash(Hash)}
     *      Gets nearest server -> traverse HashRing in CW order.
     * {@link #getServerByName(String)}
     *      Gets server by its server name. Return the server if it is
     *      either in the ring or not in ring but in servers map. Returns
     *      null if it doesn't exist in either data structures.
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
     *      Removes the server by its server name.
     * {@link #removeServer(ECSNode)}
     *      Removes the server by the server object.
     *      IMPORTANT: this method only sets the ECSNode object to STOP,
     *      it does NOT remove it from the hashRing.
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
    public abstract void removeServer(ECSNode server);
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

    public abstract TreeMap<Hash, ECSNode> getRing();
    public abstract Map<String, IECSNode> getServers();

    /**
     * {@link #serialize()}
     *      Serializes this class so that it can be passed through the
     *      network.
     * {@link #deserialize(String)}
     *      Deserializes this class. First create an empty instance, then
     *      call this function with the JSON string. This will populate
     *      HashRing by deserializing the data in the JSON.
     */
    public abstract String serialize();
    public abstract HashRing deserialize(String json);

    public abstract void print();
    //////////////////////////////////////////////////////////////
}
