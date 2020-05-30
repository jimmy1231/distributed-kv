package server.impl;

import server.DSCache;
import server.DSCache.CacheEntry;

import java.util.Map;
import java.util.Objects;

public class PolicyLFU implements DSCache.Policy {
    /**
     * Find the least frequently used entry. Assumes synchronization
     * is enforced on _cache outside of this function. Assumes _cache
     * is non-empty.
     *
     * This function should always return a non-null value.
     *
     * @param _cache The cache
     * @param key New key to be inserted
     */
    @Override
    public CacheEntry evict(Map<String, CacheEntry> _cache, String key) {
        assert(_cache.size() != 0);

        CacheEntry lfu = null;
        long least = Long.MAX_VALUE;
        for (DSCache.CacheEntry entry : _cache.values()) {
            if (Objects.isNull(lfu)) {
                lfu = entry;
                least = entry.accessFrequency;
                continue;
            }

            if (entry.accessFrequency < least) {
                lfu = entry;
                least = entry.accessFrequency;
            }
        }

        assert(Objects.nonNull(lfu));
        return lfu;
    }
}
