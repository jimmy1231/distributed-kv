package app_kvECS;

public interface SocketRequest {
    public String toJsonString();
    public SocketRequest fromJsonString(String json);
}
