package app_kvECS;

public interface SocketResponse {
    public String toJsonString();
    public SocketResponse fromJsonString(String json);
}
