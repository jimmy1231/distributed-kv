package shared.messages;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import shared.Pair;

import java.lang.reflect.Type;
import java.util.List;

public class KVDataSet {
    private static Gson KVDATA_GSON = new GsonBuilder()
        .enableComplexMapKeySerialization()
        .excludeFieldsWithoutExposeAnnotation()
        .create();

    private List<Pair<String, String>> entries;

    public KVDataSet() {
        /* Default constructor */
    }

    public KVDataSet(List<Pair<String, String>> entries) {
        this.entries = entries;
    }

    public static Gson getKvdataGson() {
        return KVDATA_GSON;
    }

    public static void setKvdataGson(Gson kvdataGson) {
        KVDATA_GSON = kvdataGson;
    }

    public List<Pair<String, String>> getEntries() {
        return entries;
    }

    public KVDataSet setEntries(List<Pair<String, String>> entries) {
        this.entries = entries;
        return this;
    }

    public String serialize() {
        System.out.println(entries);
        return KVDATA_GSON.toJson(entries);
    }

    public KVDataSet deserialize(String json) {
        Type type = new TypeToken<List<Pair<String, String>>>() {}.getType();
        this.entries = KVDATA_GSON.fromJson(json, type);

        System.out.println("DESERIALIZE!: "+ entries);
        return this;
    }

    public void print(String header) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("============%s: DATA===========\n", header));
        for (Pair<String, String> pair : entries) {
            sb.append(String.format("\tKey: %s\t\t | Data: %s\n",
                pair.getKey(), pair.getValue()));
        }
    }
}
