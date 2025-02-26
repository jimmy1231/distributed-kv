package shared.messages;

import ecs.ServerMetadata;
import ecs.impl.ServerMetadataImpl;
import server.dsmr.MapReduce;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import ecs.ECSNode;

import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

public class UnifiedMessage implements KVMessage {
    private static Gson UNIFIED_GSON = new GsonBuilder()
        .enableComplexMapKeySerialization()
        .excludeFieldsWithoutExposeAnnotation()
        .create();
    private static Gson PRINT_UNIFIED_GSON = new GsonBuilder()
        .enableComplexMapKeySerialization()
        .setPrettyPrinting()
        .excludeFieldsWithoutExposeAnnotation()
        .create();

    private class __Serialized__ {
        @Expose
        UUID uuid;
        @Expose
        MessageType messageType;
        @Expose
        KVMessage.StatusType statusType;
        @Expose
        MapReduce.Type mrType;
        @Expose
        String metadata;
        @Expose
        String dataSet;
        @Expose
        String key;
        @Expose
        String value;
        @Expose
        String[] keyRange;
        @Expose
        ECSNode server;
        @Expose
        ECSNode primary;
        @Expose
        String cacheStrategy;
        @Expose
        Integer cacheSize;
        @Expose
        String message;
        @Expose
        String[] keys;
        @Expose
        MRReport mrReport;

        __Serialized__(UUID uuid,
                       MessageType messageType,
                       KVMessage.StatusType statusType,
                       MapReduce.Type mrType,
                       String metadata,
                       String dataSet,
                       String key,
                       String value,
                       String[] keyRange,
                       ECSNode server,
                       ECSNode primary,
                       String cacheStrategy,
                       Integer cacheSize,
                       String message,
                       String[] keys,
                       MRReport mrReport) {
            this.uuid = uuid;
            this.messageType = messageType;
            this.statusType = statusType;
            this.mrType = mrType;
            this.metadata = metadata;
            this.dataSet = dataSet;
            this.key = key;
            this.value = value;
            this.keyRange = keyRange;
            this.server = server;
            this.primary = primary;
            this.cacheStrategy = cacheStrategy;
            this.cacheSize = cacheSize;
            this.message = message;
            this.keys = keys;
            this.mrReport = mrReport;
        }
    }

    ////////////////////////////////////////////////////////////////////
    // Fields of UnifiedMessage
    @Expose private UUID uuid;
    @Expose private MessageType messageType;
    @Expose private KVMessage.StatusType statusType;
    @Expose private MapReduce.Type mrType;
    private ServerMetadata metadata;
    @Expose private KVDataSet dataSet;
    @Expose private String key;
    @Expose private String value;

    @Expose private String[] keyRange;
    @Expose private ECSNode server;
    @Expose private ECSNode primary;
    @Expose private String cacheStrategy;
    @Expose private Integer cacheSize;

    @Expose private String message;

    @Expose private String[] keys;
    @Expose private MRReport mrReport;
    ////////////////////////////////////////////////////////////////////

    public static class Builder {
        UnifiedMessage object;

        public Builder() {
            object = new UnifiedMessage();
        }

        public Builder withUUID(UUID uuid){
            object.uuid = uuid;
            return this;
        }

        public Builder withMessageType(MessageType messageType) {
            object.messageType = messageType;
            return this;
        }

        public Builder withStatusType(KVMessage.StatusType statusType) {
            object.statusType = statusType;
            return this;
        }

        public Builder withMrType(MapReduce.Type mrType) {
            object.mrType = mrType;
            return this;
        }

        public Builder withMetadata(ServerMetadata metadata) {
            object.metadata = metadata;
            return this;
        }

        public Builder withDataSet(KVDataSet dataSet) {
            object.dataSet = dataSet;
            return this;
        }

        public Builder withKey(String key) {
            object.key = key;
            return this;
        }

        public Builder withValue(String value) {
            object.value = value;
            return this;
        }

        public Builder withKeyRange(String[] keyRange) {
            object.keyRange = keyRange;
            return this;
        }

        public Builder withServer(ECSNode server) {
            object.server = server;
            return this;
        }

        public Builder withPrimary(ECSNode primary) {
            object.primary = primary;
            return this;
        }

        public Builder withCacheStrategy(String cacheStrategy) {
            object.cacheStrategy = cacheStrategy;
            return this;
        }

        public Builder withCacheSize(Integer cacheSize) {
            object.cacheSize = cacheSize;
            return this;
        }

        public Builder withMessage(String message) {
            object.message = message;
            return this;
        }

        public Builder withKeys(String[] keys) {
            object.keys = keys;
            return this;
        }

        public Builder withMRReport(MRReport mrReport) {
            object.mrReport = mrReport;
            return this;
        }

        public UnifiedMessage build() {
            return object;
        }
    }

    public String serialize() {
        __Serialized__ s = new __Serialized__(
            Objects.nonNull(uuid) ? uuid : null,
            messageType,
            Objects.nonNull(statusType) ? statusType : null,
            Objects.nonNull(mrType) ? mrType : null,
            Objects.nonNull(metadata) ? metadata.serialize() : null,
            Objects.nonNull(dataSet) ? dataSet.serialize() : null,
            Objects.nonNull(key) ? key : null,
            Objects.nonNull(value) ? value : null,
            Objects.nonNull(keyRange) ? keyRange : null,
            Objects.nonNull(server) ? server : null,
            Objects.nonNull(primary) ? primary : null,
            Objects.nonNull(cacheStrategy) ? cacheStrategy : null,
            Objects.nonNull(cacheSize) ? cacheSize : null,
            Objects.nonNull(message) ? message: null,
            keys,
            Objects.nonNull(mrReport) ? mrReport : null
        );

        String str = UNIFIED_GSON.toJson(s);
        return Base64.getEncoder().encodeToString(str.getBytes());
    }

    public UnifiedMessage deserialize(String b64str) {
        String json = new String(Base64.getDecoder().decode(b64str));
        __Serialized__ s = UNIFIED_GSON.fromJson(json, __Serialized__.class);

        this.uuid = s.uuid;
        this.messageType = s.messageType;
        this.statusType = s.statusType;
        this.mrType = s.mrType;
        this.key = s.key;
        this.value = s.value;
        this.keyRange = s.keyRange;
        this.server = s.server;
        this.primary = s.primary;
        this.cacheStrategy = s.cacheStrategy;
        this.cacheSize = s.cacheSize;
        this.message = s.message;
        this.keys = s.keys;
        this.mrReport = s.mrReport;

        if (Objects.nonNull(s.metadata)) {
            this.metadata = new ServerMetadataImpl().deserialize(s.metadata);
        } else {
            this.metadata = null;
        }

        if (Objects.nonNull(s.dataSet)) {
            this.dataSet = new KVDataSet().deserialize(s.dataSet);
        } else {
            this.dataSet = null;
        }

        return this;
    }

    /////////////////////////////////////////////////////////////////////////////
    public static Gson getUnifiedGson() {
        return UNIFIED_GSON;
    }

    public static void setUnifiedGson(Gson unifiedGson) {
        UNIFIED_GSON = unifiedGson;
    }

    public UUID getUUID(){
        return uuid;
    }

    public void setUUID(UUID uuid){
        this.uuid = uuid;
    }

    public KVMessage.StatusType getStatusType() {
        return statusType;
    }

    public void setStatusType(KVMessage.StatusType statusType) {
        this.statusType = statusType;
    }

    public MapReduce.Type getMrType() {
        return mrType;
    }

    public UnifiedMessage setMrType(MapReduce.Type mrType) {
        this.mrType = mrType;
        return this;
    }

    public ServerMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(ServerMetadata metadata) {
        this.metadata = metadata;
    }

    public KVDataSet getDataSet() {
        return dataSet;
    }

    public void setDataSet(KVDataSet dataSet) {
        this.dataSet = dataSet;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public String[] getKeyRange() {
        return keyRange;
    }

    public UnifiedMessage setKeyRange(String[] keyRange) {
        this.keyRange = keyRange;
        return this;
    }

    public ECSNode getServer() {
        return server;
    }

    public UnifiedMessage setServer(ECSNode server) {
        this.server = server;
        return this;
    }

    public ECSNode getPrimary() {
        return primary;
    }

    public UnifiedMessage setPrimary(ECSNode primary) {
        this.primary = primary;
        return this;
    }

    public String getCacheStrategy() {
        return cacheStrategy;
    }

    public UnifiedMessage setCacheStrategy(String cacheStrategy) {
        this.cacheStrategy = cacheStrategy;
        return this;
    }

    public Integer getCacheSize() {
        return cacheSize;
    }

    public UnifiedMessage setCacheSize(Integer cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public UnifiedMessage setMessage(String message) {
        this.message = message;
        return this;
    }

    public UUID getUuid() {
        return uuid;
    }

    public UnifiedMessage setUuid(UUID uuid) {
        this.uuid = uuid;
        return this;
    }

    public String[] getKeys() {
        return keys;
    }

    public UnifiedMessage setKeys(String[] keys) {
        this.keys = keys;
        return this;
    }

    public MRReport getMrReport() {
        return mrReport;
    }

    public UnifiedMessage setMrReport(MRReport mrReport) {
        this.mrReport = mrReport;
        return this;
    }

    @Override
    public String toString() {
        return PRINT_UNIFIED_GSON.toJson(this);
    }

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    @Override
    public StatusType getStatus() {
        return this.statusType;
    }

    @Override
    public void setStatus(StatusType newStatus) {
        this.statusType = newStatus;
    }
    /////////////////////////////////////////////////////////////////////////////
}
