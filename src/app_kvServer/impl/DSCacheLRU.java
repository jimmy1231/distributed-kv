package app_kvServer.impl;

import app_kvServer.DSCache;

public class DSCacheLRU extends DSCache {
    public DSCacheLRU(int size) {
        super(size);
    }

    @Override
    public String getCacheStrategy() {
        return null;
    }

    @Override
    public boolean inCache(String key) {
        return false;
    }

    @Override
    public String getKV(String key) throws Exception {
        return null;
    }

    @Override
    public void putKV(String key, String value) throws Exception {

    }
}
