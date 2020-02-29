package app_kvECS;

import org.apache.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;

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
            return 0;
        }

        @Override
        public String toString() {
            return null;
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
    public abstract KVServerMetadata getServerByHash(String hash);
    public abstract KVServerMetadata getServerByName(String serverName);
    public abstract KVServerMetadata getServerByObjectKey(String objectKey);

    public abstract void removeServerByHash(String hash);
    public abstract void removeServerByName(String serverName);
    public abstract void addServer(KVServerMetadata server);

    public abstract KVServerMetadata getSuccessorServer(KVServerMetadata server);
    public abstract KVServerMetadata getPredecessorServer(KVServerMetadata server);

    public abstract HashRange getServerHashRange(KVServerMetadata server);
    public abstract HashRange getServerHashRange(String serverName);
    //////////////////////////////////////////////////////////////
}
