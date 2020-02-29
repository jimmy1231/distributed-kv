package shared.messages;

import java.io.Serializable;

/**
 * Represents a message format
 */
public class Message implements KVMessage, Serializable{
    private String key;
    private String value;
    private StatusType status;
    private Metadata metadata;

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

    public void setStatus(StatusType newStatus) {
        status = newStatus;
    }

    public void setValue(String newValue){
        value = newValue;
    }

    public void getMetadata() {
	return metadata;
    }

    public void setMetadata(Metadata newMetadata) {
	metadata = newMetadata;
    }
}
