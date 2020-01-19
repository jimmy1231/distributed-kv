package app_kvServer.impl;

import app_kvServer.DSCache;
import app_kvServer.DSCache.CacheElem;

import java.util.Map;

public class PolicyLFU implements DSCache.Policy {
    @Override
    public CacheElem evict(Map<String, CacheElem> _cache, String key) {
        return null;
    }
}
