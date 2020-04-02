package shared.messages;

import app_kvECS.HashRing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import shared.Pair;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class KVDataSet {
    private static Gson KVDATA_GSON = new GsonBuilder()
        .enableComplexMapKeySerialization()
        .excludeFieldsWithoutExposeAnnotation()
        .create();

    private List<Pair<String, String>> entries;

    public KVDataSet() {
        /* Default constructor */
        entries = new ArrayList<>();
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

    public void addEntry(Pair<String, String> entry) {
        this.entries.add(entry);
    }

    public String combineValues() {
        StringBuilder sb = new StringBuilder();
        Pair<String, String> entry;
        int i;
        for (i=0; i<this.entries.size(); i++) {
            entry = this.entries.get(i);
            sb.append(entry.getValue());

            if (i < this.entries.size()-1) {
                sb.append(" ");
            }
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return KVDATA_GSON.toJson(this);
    }

    public String serialize() {
        String str = KVDATA_GSON.toJson(entries);
        return Base64.getEncoder().encodeToString(str.getBytes());
    }

    public KVDataSet deserialize(String b64str) {
        String json = new String(Base64.getDecoder().decode(b64str));
        Type type = new TypeToken<List<Pair<String, String>>>() {}.getType();
        this.entries = KVDATA_GSON.fromJson(json, type);

        return this;
    }

    public String print(String header) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("\n============%s: DATA===========\n", header));
        int keyFormatLen = 0;
        for (Pair<String, String> pair : entries) {
            if (keyFormatLen < pair.getKey().length()) {
                keyFormatLen = pair.getKey().length();
            }
        }
        keyFormatLen+=2;

        HashRing.Hash hash;
        String key;
        String value;
        for (Pair<String, String> pair : entries) {
            key = pair.getKey();
            value = pair.getValue();
            hash = new HashRing.Hash(key);
            sb.append(String.format(
                "\tKey: %" + keyFormatLen + "s -> %s | Data: %s\t\t\n",
                key, hash.toHexString(), value));
        }

        return sb.toString();
    }
}
