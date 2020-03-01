package shared.messages;

import app_kvECS.KVServerMetadata;
import app_kvECS.impl.KVServerMetadataImpl;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

import java.util.Objects;

public class UnifiedRequestResponse implements KVMessage {
    private static Gson UNIFIED_GSON = new GsonBuilder()
        .enableComplexMapKeySerialization()
        .excludeFieldsWithoutExposeAnnotation()
        .create();

    private class __Serialized__ {
        @Expose
        MessageType messageType;
        @Expose
        KVMessage.StatusType statusType;
        @Expose
        String metadata;
        @Expose
        String dataSet;
        @Expose
        String key;
        @Expose
        String value;

        __Serialized__(MessageType messageType,
                       KVMessage.StatusType statusType,
                       String metadata,
                       String dataSet,
                       String key,
                       String value) {
            this.messageType = messageType;
            this.statusType = statusType;
            this.metadata = metadata;
            this.dataSet = dataSet;
            this.key = key;
            this.value = value;
        }
    }

    private MessageType messageType;
    private KVMessage.StatusType statusType;
    private KVServerMetadata metadata;
    private KVDataSet dataSet;
    private String key;
    private String value;

    public static class Builder {
        UnifiedRequestResponse object;

        public Builder() {
            object = new UnifiedRequestResponse();
        }

        public Builder withMessageType(MessageType messageType) {
            object.messageType = messageType;
            return this;
        }

        public Builder withStatusType(KVMessage.StatusType statusType) {
            object.statusType = statusType;
            return this;
        }

        public Builder withMetadata(KVServerMetadata metadata) {
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

        public UnifiedRequestResponse build() {
            return object;
        }
    }

    public String serialize() {
        __Serialized__ s = new __Serialized__(
            messageType,
            Objects.nonNull(statusType) ? statusType : null,
            Objects.nonNull(metadata) ? metadata.serialize() : null,
            Objects.nonNull(dataSet) ? dataSet.serialize() : null,
            Objects.nonNull(key) ? key : null,
            Objects.nonNull(value) ? value : null
        );

        return UNIFIED_GSON.toJson(s);
    }

    public UnifiedRequestResponse deserialize(String json) {
        __Serialized__ s = UNIFIED_GSON.fromJson(json, __Serialized__.class);

        this.messageType = s.messageType;
        this.statusType = s.statusType;
        this.key = s.key;
        this.value = s.value;

        if (Objects.nonNull(s.metadata)) {
            this.metadata = new KVServerMetadataImpl().deserialize(s.metadata);
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

    public KVMessage.StatusType getStatusType() {
        return statusType;
    }

    public void setStatusType(KVMessage.StatusType statusType) {
        this.statusType = statusType;
    }

    public KVServerMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(KVServerMetadata metadata) {
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
