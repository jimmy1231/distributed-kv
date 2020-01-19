package app_kvServer;

import app_kvServer.impl.PolicyFIFO;
import app_kvServer.impl.PolicyLFU;
import app_kvServer.impl.PolicyLRU;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * This is the cache to be used. It is fully concurrent.
 */
public class DSCache {
    public enum Strategy {
        LRU,
        FIFO,
        LFU
    }

    public class CacheElem {
        Date lastAccessed;
        int accessFrequency;
        String data;
    }

    @FunctionalInterface
    public interface Policy {
        /**
         * Each of the cache replacement policies will be implemented
         * with the following interface. DSCache will use one of these
         * policies.
         *
         * Note that this function DOES NOT evict the entry from the
         * cache, it simply recognizes which entry to evict. It is up
         * to the caller to subsequently perform any necessarily
         * evictions based on results of this call.
         *
         * @param _cache The cache
         * @param key New key to be inserted
         * @return The CacheElem to be evicted
         */
        CacheElem evict(Map<String, CacheElem> _cache, String key);
    }

    private Map<String, CacheElem> _cache;
    private Policy policy;
    private int cacheSize;
    private Strategy strategy;

    public DSCache(int size, String strategy) throws Exception {
        Strategy strat = Strategy.valueOf(strategy);
        switch (strat) {
            case LRU:
                policy = new PolicyLRU();
                break;
            case FIFO:
                policy = new PolicyFIFO();
                break;
            case LFU:
                policy = new PolicyLFU();
                break;
            default:
                throw new Exception("Invalid Policy: " + strategy);
        }

        this.strategy = strat;
        _cache = new HashMap<>(size);
        cacheSize = size;
    }

    public void clearCache() {
        _cache.clear();
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public boolean inCache(String key) {
        return _cache.containsKey(key);
    }

    public String getCacheStrategy() {
        return strategy.toString();
    }

    /**
     * GetKV involves cache and disk coordination. Here's the algorithm:
     * (1) Search _cache for matching key
     * (2) If matching key, return that object
     * (2) If no matching key, go to disk and fetch object (this implies
     *     the cache is full)
     *      - if cannot find in disk, throw error
     *      - if found in disk, evict an entry in cache based on
     *        policy and place the fetched data in cache.
     * @param key
     * @return
     * @throws Exception
     */
    public String getKV(String key) throws Exception {
        return null;
    }

    /**
     * PutKV involves cache and disk coordination. Here's the algorithm:
     * (1) Search _cache for matching key/entry
     * (2)
     * @param key
     * @param value
     * @throws Exception
     */
    public void putKV(String key, String value) throws Exception {

    }
}
