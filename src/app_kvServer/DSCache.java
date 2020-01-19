package app_kvServer;

import java.util.HashMap;
import java.util.Map;

/**
 * This is the cache to be used. It is fully concurrent.
 */
public abstract class DSCache {
    public enum Strategy {
        LRU,
        FIFO,
        LFU
    }

    protected Map<String, String> _cache;
    private int cacheSize;

    protected DSCache(int size) {
        _cache = new HashMap<>(size);
        cacheSize = size;
    }

    public void clearCache() {
        _cache.clear();
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public abstract String getCacheStrategy();
    public abstract boolean inCache(String key);
    public abstract String getKV(String key) throws Exception;
    public abstract void putKV(String key, String value) throws Exception;
}
