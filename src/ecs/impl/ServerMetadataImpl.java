package ecs.impl;

import ecs.HashRing;
import ecs.ServerMetadata;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import ecs.IECSNode;

import java.util.Base64;

public class ServerMetadataImpl extends ServerMetadata {
    private Gson METADATA_GSON = new GsonBuilder()
        .enableComplexMapKeySerialization()
        .excludeFieldsWithoutExposeAnnotation()
        .create();

    private class SerializedKVServerMetadata {
        @Expose
        String name;
        @Expose
        String host;
        @Expose
        String port;
        @Expose
        IECSNode.ECSNodeFlag ecsNodeFlag;
        @Expose
        String serializedHashRing;
    }

    public ServerMetadataImpl() {
        /* Default constructor */
    }

    public ServerMetadataImpl(String name, String host, IECSNode.ECSNodeFlag ecsNodeFlag, HashRing hashRing) {
        super(name, host, ecsNodeFlag, hashRing);
    }

    @Override
    public HashRing getHashRing() {
        return this.hashRing;
    }

    @Override
    public IECSNode.ECSNodeFlag getEcsNodeFlag() {
        return ecsNodeFlag;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getHost() {
        return this.host;
    }

    @Override
    public void setECSNodeFlag(IECSNode.ECSNodeFlag flag) {
        this.ecsNodeFlag = flag;
    }

    @Override
    public String serialize() {
        SerializedKVServerMetadata serialized = new SerializedKVServerMetadata();
        serialized.ecsNodeFlag = this.ecsNodeFlag;
        serialized.host = this.host;
        serialized.name = this.name;
        serialized.port = this.port;
        serialized.serializedHashRing = this.hashRing.serialize();

        String str = METADATA_GSON.toJson(serialized);
        return Base64.getEncoder().encodeToString(str.getBytes());
    }

    @Override
    public ServerMetadata deserialize(String b64str) {
        String json = new String(Base64.getDecoder().decode(b64str));
        SerializedKVServerMetadata serialized = METADATA_GSON.fromJson(
            json, SerializedKVServerMetadata.class
        );

        this.host = serialized.host;
        this.name = serialized.name;
        this.port = serialized.port;
        this.ecsNodeFlag = serialized.ecsNodeFlag;
        this.hashRing = new HashRingImpl().deserialize(serialized.serializedHashRing);

        return this;
    }
}
