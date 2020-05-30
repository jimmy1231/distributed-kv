package server.impl;

import server.DSCache;

import java.util.Map;

public class PolicyNoOp implements DSCache.Policy {
    @Override
    public DSCache.CacheEntry evict(Map<String, DSCache.CacheEntry> _cache, String key) {
        return null;
    }
}
