package shared.messages;

import java.io.Serializable;

import app_kvECS.KVServerMetadata;

/**
 * Represents a message format
 */
public class Message implements KVMessage, Serializable{
    private String key;
    private String value;
    private StatusType status;
    private KVServerMetadata metadata;

    public Message(){
        super();
    }

    public Message(String k, String v, StatusType s){
        key = k;
        value = v;
        status = s;
        metadata = null;
    }

    @Override
    public String getKey(){
        return key;
    }

    @Override
    public String getValue(){
        return value;
    }

    @Override
    public StatusType getStatus(){
        return status;
    }

    @Override
    public void setStatus(StatusType newStatus) {
        status = newStatus;
    }

    @Override
    public void setValue(String newValue){
        value = newValue;
    }

    @Override
    public KVServerMetadata getMetadata() {
	return metadata;
    }

    @Override
    public void setMetadata(KVServerMetadata newMetadata) {
	metadata = newMetadata;
    }
}
