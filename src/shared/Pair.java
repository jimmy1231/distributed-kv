package shared;

public class Pair<E1, E2> {
    private E1 key;
    private E2 value;

    public Pair(E1 key, E2 value) {
        this.key = key;
        this.value = value;
    }

    public E1 getKey() {
        return key;
    }

    public Pair<E1, E2> setKey(E1 key) {
        this.key = key;
        return this;
    }

    public E2 getValue() {
        return value;
    }

    public Pair<E1, E2> setValue(E2 value) {
        this.value = value;
        return this;
    }
}
