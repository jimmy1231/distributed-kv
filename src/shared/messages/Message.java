package shared.messages;

/**
 * Represents a message format
 */
public class Message implements KVMessage{
    private String key;
    private String value;
    private StatusType status;

    public Message(String k, String v, StatusType s){
        this.key = k;
        this.value = v;
        this.status = s;
    }

    @Override
    public String getKey(){
        return this.key;
    }

    @Override
    public String getValue(){
        return this.value;
    }

    @Override
    public StatusType getStatus(){
        return this.status;
    }
}
