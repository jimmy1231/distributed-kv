package shared;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

public class Pair<E1, E2> {
    private static Gson PAIR_GSON = new GsonBuilder()
        .enableComplexMapKeySerialization()
        .excludeFieldsWithoutExposeAnnotation()
        .create();

    @Expose
    private E1 key;
    @Expose
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

    @Override
    public String toString() {
        return PAIR_GSON.toJson(this);
    }
}
