package app_kvServer;

import app_kvServer.impl.PolicyFIFO;
import app_kvServer.impl.PolicyLFU;
import app_kvServer.impl.PolicyLRU;
import app_kvServer.impl.PolicyNoOp;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is the cache to be used. It is fully thread safe.
 */
public class DSCache {
    public class CacheEntry {
        public long lastAccessed;
        public int accessFrequency;
        public int order;
        String key;
        String data;
        Lock l;
        boolean dirty;

        CacheEntry(String _key, String _data, int _order, boolean _dirty) {
            updateAccessTime();
            accessFrequency = 1;
            key = _key;
            data = _data;
            order = _order;
            dirty = _dirty;
            l = new ReentrantLock();
        }

        void updateAccessTime() {
            lastAccessed = System.nanoTime();
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
         * @return The CacheEntry to be evicted
         */
        CacheEntry evict(Map<String, CacheEntry> _cache, String key);
    }

    private Map<String, CacheEntry> _cache;
    private Policy policy;
    private int cacheSize;
    private IKVServer.CacheStrategy strategy;
    private Lock gl;

    /* Monotonically non-decreasing number -> enforces FIFO ordering */
    private int n = 0;

    public DSCache(int size, String strategy) {
        IKVServer.CacheStrategy strat = IKVServer.CacheStrategy.valueOf(strategy);
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
                policy = new PolicyNoOp();
                size = 0;
                break;
        }

        this.strategy = strat;
        _cache = new HashMap<>();
        cacheSize = size;
        gl = new ReentrantLock();
    }

    public void clearCache() throws Exception {
        /* GLOBAL CRITICAL REGION - START */
        gl.lock();

        /* Flush all cache entries to disk */
        int cnt = 0;
        try {
            for (CacheEntry entry : _cache.values()) {
                if (entry.dirty) {
                    Disk.putKV(entry.key, entry.data);
                }
                cnt++;
            }
        } catch (Exception e) {
            throw new Exception(String.format(
                "Deleted %d elements. Error: %s",
                cnt, e.getMessage())
            );
        }

        assert(cnt == _cache.size());
        _cache.clear();

        gl.unlock();
        /* GLOBAL CRITICAL REGION - END */
    }

    public int getCacheSize() {
        /* GLOBAL CRITICAL REGION - START */
        gl.lock();

        int size = _cache.size();

        gl.unlock();
        /* GLOBAL CRITICAL REGION - END */

        return size;
    }

    public int getCacheCapacity() {
        return cacheSize;
    }

    public boolean inCache(String key) {
        /* GLOBAL CRITICAL REGION - START */
        gl.lock();

        boolean contains = _cache.containsKey(key);

        gl.unlock();
        /* GLOBAL CRITICAL REGION - END */

        return contains;
    }

    public IKVServer.CacheStrategy getCacheStrategy() {
        return strategy;
    }

    /**
     * Locking scheme:
     * ------------------------------------------------------------------
     * (1) Lock global -> guarantees 'get' from $ is atomic
     * (2) If entry exists, then lock entry. Note that the entry is lock-
     *     ed while global is still locked. The global lock ensures that
     *     no other threads evict the entry between the time when entry
     *     is accessed from $ and when the entry is locked.
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
     *
     * @throws Exception Generic program runtime error. This should be
     * handled gracefully
     * @throws AssertionError Assert returned false.
     * !!CRASH THE PROGRAM!!
     */
    public String getKV(String key) throws AssertionError, Exception {
        /* GLOBAL CRITICAL REGION - START */
        gl.lock();

        CacheEntry entry = null;
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

        /*
         * Look in the disk to see if entry was evicted. Yes, we
         * are essentially doing an evict-put (same as putKV), but
         * can't call putKV because we have to ensure atomicity, so
         * have to replicate putKV code here.
         */
        if (Objects.nonNull(data = Disk.getKV(key))) {
            /* Special case - no cache used */
            if (cacheSize == 0) {
                assert(_cache.size() == 0);
                gl.unlock();
                /* GLOBAL CRITICAL REGION - END */

                return data;
            }

            entry = new CacheEntry(key, data, n, false);
            n++;
            if (_cache.size() < cacheSize) {
                _cache.put(key, entry);
            } else {
                CacheEntry evict = policy.evict(_cache, key);

                /* Only write to disk if disk data is stale */
                if (evict.dirty) {
                    try {
                        Disk.putKV(evict.key, evict.data);
                    } catch (Exception e) {
                        n--;
                        gl.unlock();
                        /* GLOBAL CRITICAL REGION - END */

                        throw new Exception(String.format(
                            "Error upon evicting object with key %s -> %s",
                            key, e.getMessage()
                        ));
                    }
                }

                _cache.remove(evict.key);
                _cache.put(key, entry);
            }

            gl.unlock();
            /* GLOBAL CRITICAL REGION - END */

            return data;
        }

        /* Element doesn't exist anywhere */
        gl.unlock();
        /* GLOBAL CRITICAL REGION - END */

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
     * @throws Exception Generic program runtime error. This should be
     * handled gracefully
     * @throws AssertionError Assert returned false.
     * !!CRASH THE PROGRAM!!
     */
    public void putKV(String key, String value) throws AssertionError, Exception {
        /* GLOBAL CRITICAL REGION - START */
        gl.lock();

        /*
         * 2 Cases in general:
         * (1) Entry with 'key' already exists in cache -> update
         * (2) Entry with 'key' does not yet exist in cache -> insert
         */

        // (1)
        CacheEntry entry;
        if (Objects.nonNull(entry = _cache.get(key))) {
            /* ENTRY CRITICAL REGION - START */
            entry.l.lock();

            gl.unlock();
            /* GLOBAL CRITICAL REGION - END */

            entry.data = value;
            entry.accessFrequency++;
            entry.updateAccessTime();
            entry.dirty = true;

            entry.l.unlock();
            /* ENTRY CRITICAL REGION - END */

            return;
        }

        // (2)
        if (_cache.size() < cacheSize) {
            entry = new CacheEntry(key, value, n, true);
            n++;
            _cache.put(key, entry);

            gl.unlock();
            /* GLOBAL CRITICAL REGION - END */

            return;
        }

        /*
         * Special case: cacheSize is 0 means we're caching -> persist
         * data directly to disk.
         */
        if (cacheSize == 0) {
            assert(_cache.size() == 0);
            try {
                Disk.putKV(key, value);
            } catch (Exception e) {
                throw new Exception(String.format(
                    "Direct persistence to disk error: %s",
                    e.getMessage()
                ));
            } finally {
                gl.unlock();
                /* GLOBAL CRITICAL REGION - END */
            }

            return;
        }

        /*
         * Cache has been filled, need to evict an entry. Assumes
         * disk handles its own synchronization. Notice that evict
         * is locked and unlocked. This is to prevent read/write race
         * conditions during eviction.
         *
         * IMPORTANT: Always lock the entry before evicting! Acquiring
         * the lock guarantees that no other threads are working on the
         * entry to be evicted.
         */
        assert(_cache.size() == cacheSize);
        CacheEntry evict = policy.evict(_cache, key);

        /* ENTRY CRITICAL REGION - START */
        evict.l.lock();

        /* Only write to disk if disk data is stale */
        if (evict.dirty) {
            try {
                Disk.putKV(evict.key, evict.data);
            } catch (Exception e) {
                evict.l.unlock();
                gl.unlock();
                /* GLOBAL/ENTRY CRITICAL REGION - END */

                throw new Exception(String.format(
                    "Error evicting object: %s. %s",
                    key, e.getMessage()
                ));
            }
        }

        _cache.remove(evict.key);
        evict.l.unlock();
        /* ENTRY CRITICAL REGION - END */

        entry = new CacheEntry(key, value, n, true);
        n++;
        _cache.put(key, entry);
        if (_cache.size() != cacheSize) {
            System.out.println(String.format(
                "Expecting cache size to be: %d, actual: %d",
                cacheSize, _cache.size()
            ));
        }
        assert(_cache.size() == cacheSize);

        gl.unlock();
        /* GLOBAL CRITICAL REGION - END */
    }

    public void dumpCache() {
        StringBuilder sb = new StringBuilder();
        sb.append("Key   Data                                     LastModified Frequency Order\n");
        sb.append("-------------------------------------------------------------------------\n");
        for (CacheEntry ce : _cache.values()) {
            sb.append(dumpEntry(ce));
        }
        System.out.println(sb.toString());
    }

    private static String dumpEntry(CacheEntry ce) {
        return String.format("%-5s %-40s %-12d %-9d %d\n",
            ce.key, ce.data, ce.lastAccessed,
            ce.accessFrequency, ce.order
        );
    }
}
