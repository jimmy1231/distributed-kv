package app_kvECS.impl;

import app_kvECS.HashRing;
import app_kvECS.KVServerMetadata;
import ecs.IECSNode;

public class KVServerMetadataImpl extends KVServerMetadata {
    public KVServerMetadataImpl(String name, String host, IECSNode.ECSNodeFlag ecsNodeFlag, HashRing hashRing) {
        super(name, host, ecsNodeFlag, hashRing);
    }

    @Override
    public HashRing getHashRing() {
        return null;
    }

    @Override
    public IECSNode.ECSNodeFlag getEcsNodeFlag() {
        return ecsNodeFlag;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getHost() {
        return null;
    }

    @Override
    public String serialize() {
        return null;
    }

    @Override
    public void setECSNodeFlag(IECSNode.ECSNodeFlag flag) {
        this.ecsNodeFlag = flag;
    }

}
