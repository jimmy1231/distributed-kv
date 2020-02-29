package app_kvECS.impl;

import app_kvECS.HashRing;
import app_kvECS.KVServerMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class HashRingImpl extends HashRing {
    public HashRingImpl() {
        super(new TreeMap<>(), new HashMap<>());
    }

    @Override
    public KVServerMetadata getServerByHash(String hash) {
        return null;
    }

    @Override
    public KVServerMetadata getServerByName(String serverName) {
        return null;
    }

    @Override
    public KVServerMetadata getServerByObjectKey(String objectKey) {
        return null;
    }

    @Override
    public void removeServerByHash(String hash) {

    }

    @Override
    public void removeServerByName(String serverName) {

    }

    @Override
    public void addServer(KVServerMetadata server) {

    }

    @Override
    public KVServerMetadata getSuccessorServer(KVServerMetadata server) {
        return null;
    }

    @Override
    public KVServerMetadata getPredecessorServer(KVServerMetadata server) {
        return null;
    }

    @Override
    public HashRange getServerHashRange(KVServerMetadata server) {
        return null;
    }

    @Override
    public HashRange getServerHashRange(String serverName) {
        return null;
    }
}
