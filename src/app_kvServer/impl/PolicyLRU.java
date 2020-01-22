package app_kvServer.impl;

import app_kvServer.DSCache;
import app_kvServer.DSCache.CacheEntry;

import java.util.Map;
import java.util.Objects;

public class PolicyLRU implements DSCache.Policy {
    /**
     * Find the oldest entry. Assumes synchronization on _cache
     * outside of this function. Assumes _cache is non-empty.
     *
     * This function should always return a non-null value.
     *
     * @param _cache The cache
     * @param key New key to be inserted
     */
    @Override
    public DSCache.CacheEntry evict(Map<String, CacheEntry> _cache, String key) {
        assert(_cache.size() != 0);

        CacheEntry oldest = null;
        long oldestTime = Long.MAX_VALUE;
        for (CacheEntry entry : _cache.values()) {
            if (Objects.isNull(oldest)) {
                oldest = entry;
                oldestTime = entry.lastAccessed;
                continue;
            }

            if (entry.lastAccessed < oldestTime) {
                oldest = entry;
                oldestTime = entry.lastAccessed;
            }
        }

        assert(Objects.nonNull(oldest));
        return oldest;
    }
}
