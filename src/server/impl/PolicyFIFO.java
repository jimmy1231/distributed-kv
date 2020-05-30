package server.impl;

import server.DSCache;
import server.DSCache.CacheEntry;

import java.util.Map;
import java.util.Objects;

public class PolicyFIFO implements DSCache.Policy {
    /**
     * Find the entry with smallest order -> the first-in entry.
     * Assumes synchronization on _cache outside of this function.
     * Assumes _cache is non-empty.
     *
     * This function should always return a non-null value.
     *
     * @param _cache The cache
     * @param key New key to be inserted
     */
    @Override
    public CacheEntry evict(Map<String, CacheEntry> _cache, String key) {
        assert(_cache.size() != 0);

        CacheEntry first = null;
        long leastOrder = Long.MAX_VALUE;
        for (CacheEntry entry : _cache.values()) {
            if (Objects.isNull(first)) {
                first = entry;
                leastOrder = entry.order;
                continue;
            }

            if (entry.order < leastOrder) {
                first = entry;
                leastOrder = entry.lastAccessed;
            }
        }

        assert(Objects.nonNull(first));
        return first;
    }
}
