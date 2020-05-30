package server.dsmr;

public class MapInput {
    private String value;

    public MapInput(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public MapInput setValue(String value) {
        this.value = value;
        return this;
    }
}
