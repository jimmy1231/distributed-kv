package app_kvServer.impl;

import app_kvServer.DSCache;
import app_kvServer.DSCache.CacheElem;

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
    public CacheElem evict(Map<String, CacheElem> _cache, String key) {
        assert(_cache.size() != 0);

        CacheElem oldest = null;
        long oldestTime = Long.MAX_VALUE;
        for (CacheElem entry : _cache.values()) {
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
