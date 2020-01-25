package shared.messages;

import java.io.Serializable;

/**
 * Represents a message format
 */
public class Message implements KVMessage, Serializable{
    private String key;
    private String value;
    private StatusType status;

    public Message(){
        super();
    }

    public Message(String k, String v, StatusType s){
        key = k;
        value = v;
        status = s;
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
}
