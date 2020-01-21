package app_kvServer;

import app_kvServer.impl.PolicyFIFO;
import app_kvServer.impl.PolicyLFU;
import app_kvServer.impl.PolicyLRU;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is the cache to be used. It is fully concurrent.
 */
public class DSCache {
    public enum Strategy { LRU, FIFO, LFU }

    public class CacheElem {
        long lastAccessed;
        int accessFrequency;
        String key;
        String data;
        Lock l;

        CacheElem(String _key, String _data) {
            updateAccessTime();
            accessFrequency = 0;
            key = _key;
            data = _data;
            l = new ReentrantLock();
        }

        void updateAccessTime() {
            lastAccessed = System.currentTimeMillis();
        }
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
    private Lock gl;

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
        _cache = new HashMap<>();
        cacheSize = size;
        gl = new ReentrantLock();
    }

    public void clearCache() {
        _cache.clear();
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public boolean inCache(String key) {
        gl.lock();
        boolean contains = _cache.containsKey(key);
        gl.unlock();
        return contains;
    }

    public String getCacheStrategy() {
        return strategy.toString();
    }

    /**
     * Locking scheme:
     * ------------------------------------------------------------------
     * (1) Lock global -> guarantees 'get' from $ is atomic
     * (2) If entry exists, then lock entry. Note that the entry is locked
     *     while global is still locked. The global lock ensures that no
     *     other threads evict the entry between the time when entry is
     *     accessed from $ and when the entry is locked.
     * (3) Unlock global -> once entry is locked, the entry can no longer
     *     be evicted. We can safely perform entry-wise operations.
     *
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
        /* GLOBAL CRITICAL REGION - START */
        gl.lock();

        CacheElem entry = null;
        String data = null;

        if (Objects.nonNull(entry = _cache.get(key))) {
            /* ENTRY CRITICAL REGION - START */
            entry.l.lock();

            gl.unlock();
            /* GLOBAL CRITICAL REGION - END */

            entry.updateAccessTime();
            entry.accessFrequency++;
            data = entry.data;

            entry.l.unlock();
            /* ENTRY CRITICAL REGION - END */

            return data;
        }

        gl.unlock();
        /* GLOBAL CRITICAL REGION - END */

        /* Look in the disk to see if entry was evicted */
        if (Objects.nonNull(data = Disk.getKV(key))) {
            putKV(key, data);
            return data;
        }

        /* Element doesn't exist anywhere */
        return null;
    }

    /**
     * PutKV involves cache and disk coordination. Here's the algorithm:
     * (1) Search _cache for matching key/entry
     * (2) If entry is present, update that entry
     * (3) If entry is not present, insert that entry
     *
     * Note: Synchronization is guaranteed; The configured replacement
     * policy is respected when the cache is full.
     *
     * @param key
     * @param value
     * @throws Exception
     */
    public void putKV(String key, String value) throws Exception {
        /* GLOBAL CRITICAL REGION - START */
        gl.lock();

        CacheElem entry;
        if (_cache.size() < cacheSize) {
            /*
             * Cache has not yet been filled up. 2 cases:
             * (1) Entry with 'key' already exists in cache -> update
             * (2) Entry with 'key' does not yet exist in cache -> insert
             */

            // (1)
            if (Objects.nonNull(entry = _cache.get(key))) {
                /* ENTRY CRITICAL REGION - START */
                entry.l.lock();

                gl.unlock();
                /* GLOBAL CRITICAL REGION - END */

                entry.data = value;
                entry.accessFrequency++;
                entry.updateAccessTime();

                entry.l.unlock();
                /* ENTRY CRITICAL REGION - END */
            }
            // (2)
            else {
                entry = new CacheElem(key, value);
                _cache.put(key, entry);

                gl.unlock();
                /* GLOBAL CRITICAL REGION - END */
            }

            return;
        }

        /*
         * Cache has been filled, need to evict an entry. Assumes
         * disk handles its own synchronization. Notice that entry2Evict
         * is locked and unlocked. This is to prevent read/write race
         * conditions during eviction.
         *
         * IMPORTANT: Always lock the entry before evicting! Acquiring
         * the lock guarantees that no other threads are working on the
         * entry to be evicted.
         */
        assert(_cache.size() == cacheSize);
        CacheElem entry2Evict = policy.evict(_cache, key);

        /* ENTRY CRITICAL REGION - START */
        entry2Evict.l.lock();

        Disk.putKV(entry2Evict.key, entry2Evict.data);
        _cache.remove(entry2Evict.key);

        entry2Evict.l.unlock();
        /* ENTRY CRITICAL REGION - END */

        entry = new CacheElem(key, value);
        _cache.put(key, entry);
        assert(_cache.size() == cacheSize);

        gl.unlock();
        /* GLOBAL CRITICAL REGION - END */
    }
}
